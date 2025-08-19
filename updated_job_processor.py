import json
import logging
from postgres_setup import PostgresSkillsPipeline
from skills_hierarchy_definitions import skill_hierarchy

class JobProcessor:
    def __init__(self):
        self.db_postgres = PostgresSkillsPipeline()
        
    def extract_skills_from_job(self, job_data):
        """Extract all skills from job competencies"""
        skills = set()
        
        # Navigate to competencies
        attributes = job_data.get('attributes', {})
        if isinstance(attributes, dict) and 'attributes' in attributes:
            attributes = attributes['attributes']
        
        competencies = attributes.get('competencies', [])
        
        for competency in competencies:
            if isinstance(competency, dict):
                comp_skills = competency.get('skills', [])
                if isinstance(comp_skills, list):
                    for skill in comp_skills:
                        if isinstance(skill, str) and skill.strip():
                            skills.add(skill.strip().lower())
        
        return list(skills)

    def insert_skills_to_db(self, skills_list, job_id, skill_name_to_id_accumulator=None):
        """Insert skills into skill table and return their IDs"""
        cursor = self.db_postgres.get_cursor()
        skill_ids = []
        
        for skill_name in skills_list:
            # Check if skill already exists
            cursor.execute("SELECT id FROM skill WHERE name = %s", (skill_name,))
            existing_skill = cursor.fetchone()
            
            if existing_skill:
                skill_id = existing_skill['id']
                skill_ids.append(skill_id)
            else:
                # Insert new skill with full data
                cursor.execute("""
                    INSERT INTO skill (name, description, metadata)
                    VALUES (%s, %s, %s)
                    RETURNING id
                """, (
                    skill_name,
                    f"Skill extracted from job {job_id}",
                    json.dumps({
                        "source": f"job_{job_id}", 
                        "extracted_date": "2024",
                        "skill_type": "technical",
                        "frequency": 1
                    })
                ))
                skill_id = cursor.fetchone()['id']
                skill_ids.append(skill_id)
            
            # Add to accumulator if provided
            if skill_name_to_id_accumulator is not None:
                skill_name_to_id_accumulator[skill_name] = skill_id
        
        return skill_ids

    def create_skillgroup_with_skills(self, skills_list, skill_ids, job_id):
        """Create skillgroup with all required fields"""
        cursor = self.db_postgres.get_cursor()
        
        skillgroup_name = f"job_{job_id}_skillgroup"
        
        # Calculate total weight (could be based on number of skills or other logic)
        total_weight = len(skills_list) * 10  # Example weighting
        
        cursor.execute("""
            INSERT INTO skillgroup (
                grouptype, name, description, skillids, weight, metadata, scoretosfiathreshold
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            'portfolio',  # evidence_type enum value
            skillgroup_name,
            f"Skill group for job {job_id} containing {len(skills_list)} skills",
            skill_ids[0] if skill_ids else None,  # Store first skill ID as reference
            total_weight,
            json.dumps({
                "skills": skills_list, 
                "skill_ids": skill_ids,
                "job_id": job_id,
                "skill_count": len(skills_list),
                "created_from": "job_extraction"
            }),
            json.dumps({
                "level_1": 20,
                "level_2": 40, 
                "level_3": 60,
                "level_4": 80,
                "level_5": 100,
                "threshold_type": "percentage"
            })
        ))
        
        return cursor.fetchone()['id']

    def extract_competency_data(self, job_data):
        """Extract competency information with SFIA levels"""
        competency_data = {}
        
        attributes = job_data.get('attributes', {})
        if isinstance(attributes, dict) and 'attributes' in attributes:
            attributes = attributes['attributes']
        
        competencies = attributes.get('competencies', [])
        
        for competency in competencies:
            if isinstance(competency, dict):
                comp_name = competency.get('name', '')
                sfia_level = competency.get('sfia_level', 1)
                skills = competency.get('skills', [])
                summary = competency.get('summary', '')
                relevance = competency.get('relevance', '')
                
                if comp_name:
                    competency_data[comp_name] = {
                        'sfia_level': sfia_level,
                        'skills': skills,
                        'summary': summary,
                        'relevance': relevance,
                        'skill_count': len(skills) if isinstance(skills, list) else 0
                    }
        
        return competency_data

    def insert_job_complete(self, job_data, skill_name_to_id_accumulator=None):
        """Insert job with complete skillgroup and skill relationships"""
        cursor = self.db_postgres.get_cursor()
        
        job_id = job_data.get('id')
        if not job_id:
            self.db_postgres.logger.error("No job ID found")
            return False
        
        # Check if job already exists
        cursor.execute("SELECT id, skillgroup FROM job WHERE id = %s", (job_id,))
        existing_job = cursor.fetchone()
        
        try:
            # Step 1: Always extract and insert skills
            skills_list = self.extract_skills_from_job(job_data)
            if not skills_list:
                self.db_postgres.logger.warning(f"No skills found for job {job_id}")
                return False
            
            skill_ids = self.insert_skills_to_db(skills_list, job_id, skill_name_to_id_accumulator)
            
            # Step 2: Create skillgroup if job doesn't exist or has no skillgroup
            if not existing_job or existing_job['skillgroup'] is None:
                skillgroup_id = self.create_skillgroup_with_skills(skills_list, skill_ids, job_id)
            else:
                skillgroup_id = existing_job['skillgroup']
                self.db_postgres.logger.info(f"Job {job_id} already has skillgroup {skillgroup_id}")
            
            # Step 3: Insert or update job
            if existing_job:
                # Update existing job with skillgroup if it was missing
                if existing_job['skillgroup'] is None:
                    competency_data = self.extract_competency_data(job_data)
                    cursor.execute("""
                        UPDATE job SET sfialevel = %s, skillgroup = %s WHERE id = %s
                    """, (json.dumps(competency_data), skillgroup_id, job_id))
                    self.db_postgres.logger.info(f"Updated existing job {job_id} with skillgroup")
                else:
                    self.db_postgres.logger.info(f"Job {job_id} already complete, skipping")
            else:
                # Insert new job
                competency_data = self.extract_competency_data(job_data)
                cursor.execute("""
                    INSERT INTO job (id, sfialevel, skillgroup)
                    VALUES (%s, %s, %s)
                """, (job_id, json.dumps(competency_data), skillgroup_id))
                self.db_postgres.logger.info(f"Inserted new job {job_id}")
            
            return True
            
        except Exception as e:
            self.db_postgres.logger.error(f"Error processing job {job_id}: {e}")
            return False

    def populate_skill_relationships(self, skill_name_to_id: dict, hierarchy_dict: dict = None):
        """
        Populates the 'skillrelationship' table based on a hierarchy dictionary.
        
        Args:
            skill_name_to_id (dict): Dictionary mapping skill names to their IDs
            hierarchy_dict (dict): A dictionary defining the parent-child skill relationships.
                                 If None, uses the default skill_hierarchy from skills_hierarchy_definitions.
        """
        if hierarchy_dict is None:
            hierarchy_dict = skill_hierarchy
            
        logger = self.db_postgres.logger
        cursor = self.db_postgres.get_cursor()
        if not cursor:
            logger.error("Failed to get a database cursor. Exiting.")
            return False
        
        try:
            # Insert relationships into the 'skillrelationship' table using provided skill mapping
            logger.info("Inserting skill relationships...")
            relationships_added = 0
            relationships_skipped = 0
            
            for parent_name, children_names in hierarchy_dict.items():
                parent_id = skill_name_to_id.get(parent_name.lower())
                if not parent_id:
                    logger.debug(f"Parent skill '{parent_name}' not found in extracted skills. Skipping relationships.")
                    continue
                
                for child_name in children_names:
                    child_id = skill_name_to_id.get(child_name.lower())
                    if not child_id:
                        logger.debug(f"Child skill '{child_name}' not found in extracted skills. Skipping relationship.")
                        continue
                    
                    # Check for existing relationship to prevent duplicates
                    cursor.execute(
                        "SELECT id FROM skillrelationship WHERE parentid = %s AND childid = %s",
                        (parent_id, child_id)
                    )
                    if cursor.fetchone() is None:
                        cursor.execute(
                            """INSERT INTO skillrelationship (parentid, childid, weight, metadata) 
                               VALUES (%s, %s, %s, %s)""",
                            (parent_id, child_id, 1, json.dumps({
                                "relationship_type": "hierarchy",
                                "created_from": "skill_hierarchy_definitions"
                            }))
                        )
                        logger.info(f"Inserted relationship: Parent '{parent_name}' -> Child '{child_name}'")
                        relationships_added += 1
                    else:
                        logger.debug(f"Relationship already exists for '{parent_name}' -> '{child_name}'. Skipping.")
                        relationships_skipped += 1
            
            logger.info(f"Skill relationships population complete. Added: {relationships_added}, Skipped: {relationships_skipped}")
            return True
            
        except Exception as e:
            logger.error(f"Error populating skill relationships: {e}")
            return False

    def process_all_jobs(self, json_file_path):
        """Process all jobs from JSON file and populate all tables including skill relationships"""
        try:
            with open(json_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Handle nested structure
            if isinstance(data, list) and len(data) > 0:
                if isinstance(data[0], list):
                    jobs = data[0]
                elif isinstance(data[0], dict) and 'data' in data[0]:
                    jobs = data[0]['data']
                else:
                    jobs = data
            else:
                jobs = data if isinstance(data, list) else [data]
            
            processed_count = 0
            failed_count = 0
            total_jobs = len(jobs)
            
            # Accumulate all skills and their IDs as we process jobs
            all_skills_to_ids = {}
            
            self.db_postgres.logger.info(f"Starting to process {total_jobs} jobs")
            
            # Process all jobs and collect skills simultaneously
            for i, job in enumerate(jobs, 1):
                if isinstance(job, dict):
                    if self.insert_job_complete(job, all_skills_to_ids):
                        processed_count += 1
                    else:
                        failed_count += 1
                
                # Progress logging
                if i % 10 == 0:
                    self.db_postgres.logger.info(f"Processed {i}/{total_jobs} jobs")
            
            self.db_postgres.logger.info(
                f"Completed job processing: {processed_count} successful, {failed_count} failed out of {total_jobs} total"
            )
            
            # Now populate skill relationships using the skills we collected during processing
            self.db_postgres.logger.info(f"Populating skill relationships using {len(all_skills_to_ids)} extracted skills...")
            if self.populate_skill_relationships(all_skills_to_ids):
                self.db_postgres.logger.info("Skill relationships populated successfully!")
            else:
                self.db_postgres.logger.error("Failed to populate skill relationships")
            
            return processed_count
            
        except Exception as e:
            self.db_postgres.logger.error(f"Error processing jobs file: {e}")
            return 0

    def get_processing_summary(self):
        """Get summary of processed data"""
        cursor = self.db_postgres.get_cursor()
        
        # Count records in each table
        cursor.execute("SELECT COUNT(*) as count FROM job")
        job_count = cursor.fetchone()['count']
        
        cursor.execute("SELECT COUNT(*) as count FROM skill")
        skill_count = cursor.fetchone()['count']
        
        cursor.execute("SELECT COUNT(*) as count FROM skillgroup")
        skillgroup_count = cursor.fetchone()['count']
        
        cursor.execute("SELECT COUNT(*) as count FROM skillrelationship")
        relationship_count = cursor.fetchone()['count']
        
        return {
            'jobs': job_count,
            'skills': skill_count,
            'skillgroups': skillgroup_count,
            'skill_relationships': relationship_count
        }

    def close_connection(self):
        """Close database connection"""
        self.db_postgres.close()
        
    def clear_and_restart(self):
        """Clear all job-related data and restart fresh"""
        cursor = self.db_postgres.get_cursor()
        
        # Clear tables in reverse dependency order
        cursor.execute("DELETE FROM job")
        cursor.execute("DELETE FROM skillgroup") 
        cursor.execute("DELETE FROM skillrelationship")
        cursor.execute("DELETE FROM skill")
        
        # Reset sequences to start from 1
        cursor.execute("ALTER SEQUENCE skill_id_seq RESTART WITH 1")
        cursor.execute("ALTER SEQUENCE skillgroup_id_seq RESTART WITH 1") 
        cursor.execute("ALTER SEQUENCE skillrelationship_id_seq RESTART WITH 1")
        
        self.db_postgres.logger.info("Cleared all job, skillgroup, skill, and relationship data")

if __name__ == "__main__":
    processor = JobProcessor()
    processor.clear_and_restart()
    
    print("Starting job processing pipeline...")
    processed_count = processor.process_all_jobs("../jobs.json")
    
    summary = processor.get_processing_summary()
    print(f"\nProcessing completed!")
    print(f"Processed {processed_count} jobs")
    print(f"Database summary:")
    print(f"  - Jobs: {summary['jobs']}")
    print(f"  - Skills: {summary['skills']}")
    print(f"  - Skill Groups: {summary['skillgroups']}")
    print(f"  - Skill Relationships: {summary['skill_relationships']}")
    
    processor.close_connection()
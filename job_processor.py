import json
import logging
from collections import defaultdict
from postgres_setup import PostgresSkillsPipeline
from skills_hierarchy_definitions import skill_hierarchy

class JobProcessor:
    def __init__(self):
        self.db_postgres = PostgresSkillsPipeline()
        self.competency_to_skillgroups = defaultdict(list)  # Track competency -> skillgroup mappings
        
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

    def create_skillgroup_with_skills(self, skills_list, skill_ids, job_id, competency_data):
        """Create skillgroup with all required fields"""
        cursor = self.db_postgres.get_cursor()
        
        skillgroup_name = f"job_{job_id}_skillgroup"
        
        # Extract SFIA levels from the provided competency data
        sfia_levels = {}
        for comp_name, details in competency_data.items():
            if 'sfia_level' in details and details['sfia_level']:
                new_key = f"{comp_name}_sfia_level"
                sfia_levels[new_key] = details['sfia_level']
                
        # Prepare the metadata
        metadata = {
            "source_job_id": job_id,
            "scoretosfiathreshold": {
                "level_1": 20,
                "level_2": 40, 
                "level_3": 60,
                "level_4": 80,
                "level_5": 100,
                "threshold_type": "percentage"
            }
        }
        
        cursor.execute("""
            INSERT INTO skillgroup (
                grouptype, name, description, skillids, skillweight, metadata, scoretosfiathreshold
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            'portfolio',  # evidence_type enum value
            skillgroup_name,
            f"Skill group for job {job_id} containing {len(skills_list)} skills",
            skill_ids if skill_ids else None,  # Store all skill IDs as reference
            json.dumps(sfia_levels), # Store the SFIA levels in the skillweight column
            json.dumps(metadata),
            None
        ))
        skillgroup_id = cursor.fetchone()['id']
        
        # Track competencies for this skillgroup
        for comp_name in competency_data.keys():
            self.competency_to_skillgroups[comp_name].append(skillgroup_id)
        
        self.db_postgres.conn.commit()
        self.db_postgres.logger.info(f"Created skillgroup {skillgroup_id} for job {job_id}")
        return skillgroup_id

    def extract_competency_data(self, job_data: dict) -> dict:
        """
        Extracts competency data from job JSON, handling different skill formats.
        
        Args:
            job_data (dict): The job data dictionary.
        
        Returns:
            dict: A dictionary of competency names mapping to their details.
        """
        competency_details = {}
        attributes = job_data.get('attributes', {})
        if isinstance(attributes, dict) and 'attributes' in attributes:
            attributes = attributes['attributes']
        
        competencies = attributes.get('competencies', [])
        
        for competency in competencies:
            if isinstance(competency, dict):
                comp_name = competency.get('name')
                if not comp_name:
                    continue
                
                skills_list = []
                comp_skills = competency.get('skills', [])
                for skill_item in comp_skills:
                    if isinstance(skill_item, dict) and 'name' in skill_item:
                        skills_list.append(skill_item)
                    elif isinstance(skill_item, str):
                        # Convert string skills to a dictionary format
                        skills_list.append({'name': skill_item})
                
                competency_details[comp_name] = {
                    "skills": skills_list,
                    "sfia_level": competency.get('sfia_level'),
                    "summary": competency.get('summary'),
                    "relevance": competency.get('relevance')
                }
        return competency_details

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

            # Step 2: Extract competency data
            competency_data = self.extract_competency_data(job_data)
            
            # Step 3: Create skillgroup if job doesn't exist or has no skillgroup
            if not existing_job or existing_job['skillgroup'] is None:
                skillgroup_id = self.create_skillgroup_with_skills(skills_list, skill_ids, job_id, competency_data)
            else:
                skillgroup_id = existing_job['skillgroup']
                # Still need to track competencies for existing skillgroups
                for comp_name in competency_data.keys():
                    if skillgroup_id not in self.competency_to_skillgroups[comp_name]:
                        self.competency_to_skillgroups[comp_name].append(skillgroup_id)
                self.db_postgres.logger.info(f"Job {job_id} already has skillgroup {skillgroup_id}")
            
            # Step 4: Insert or update job
            if existing_job:
                # Update existing job with skillgroup if it was missing
                if existing_job['skillgroup'] is None:
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
        
    def insert_competencies(self):
        """
        Process all competencies after all jobs are loaded to populate skillgroup arrays and weights.
        This should be called AFTER all jobs have been processed.
        """
        cursor = self.db_postgres.get_cursor()
        if not cursor:
            self.db_postgres.logger.error("Failed to get a database cursor.")
            return False

        self.db_postgres.logger.info("Finalizing competencies with skillgroup mappings...")
        
        try:
            updated_count = 0
            
            for comp_name, skillgroup_ids in self.competency_to_skillgroups.items():
                # Remove duplicates and sort for consistency
                unique_skillgroup_ids = sorted(list(set(skillgroup_ids)))
                
                # Calculate weights if competency appears in multiple skillgroups
                skillgroup_weights = {}
                if len(unique_skillgroup_ids) > 1:
                    # For now, assign equal weights. This could be enhanced later with more sophisticated logic
                    base_weight = 1.0 / len(unique_skillgroup_ids)
                    for sg_id in unique_skillgroup_ids:
                        skillgroup_weights[str(sg_id)] = round(base_weight, 3)
                # If only one skillgroup, leave weights empty as requested
                
                # Check if competency already exists
                cursor.execute("SELECT id FROM competency WHERE name = %s", (comp_name,))
                existing_comp = cursor.fetchone()

                if existing_comp:
                    # Update existing competency
                    cursor.execute("""
                        UPDATE competency 
                        SET skillgroup = %s, skillgroupweight = %s 
                        WHERE name = %s
                    """, (
                        unique_skillgroup_ids,
                        json.dumps(skillgroup_weights) if skillgroup_weights else None,
                        comp_name
                    ))
                    self.db_postgres.logger.info(f"Updated competency '{comp_name}' with {len(unique_skillgroup_ids)} skillgroups")
                else:
                    # Insert new competency with proper skillgroup data
                    cursor.execute("""
                        INSERT INTO competency (
                            name,
                            skillgroup,
                            skillgroupweight,
                            evidence
                        ) VALUES (%s, %s, %s, %s)
                    """, (
                        comp_name,
                        unique_skillgroup_ids,
                        json.dumps(skillgroup_weights) if skillgroup_weights else None,
                        None
                    ))
                    self.db_postgres.logger.info(f"Created competency '{comp_name}' with {len(unique_skillgroup_ids)} skillgroups")
                
                updated_count += 1
            
            self.db_postgres.conn.commit()
            self.db_postgres.logger.info(f"Finalized {updated_count} competencies with skillgroup mappings")
            return True
            
        except Exception as e:
            self.db_postgres.logger.error(f"Error finalizing competencies: {e}")
            self.db_postgres.conn.rollback()
            return False    

    def populate_skill_relationships(self, skill_name_to_id: dict, hierarchy_dict: dict = None, use_weights: bool = True):
        """
        Populates ALL relationships from hierarchy first, creating missing skills as needed.
        Then handles job-extracted skills that weren't in hierarchy.
        
        Args:
            skill_name_to_id (dict): Dictionary mapping skill names to their IDs from job extraction
            hierarchy_dict (dict): A dictionary defining the parent-child skill relationships.
            use_weights (bool): Whether to use the weighted format or simple format
        """
        # Import the hierarchy
        try:
            if use_weights:
                from skills_hierarchy_definitions import skill_hierarchy_with_weights
                hierarchy_dict = skill_hierarchy_with_weights
            else:
                from skills_hierarchy_definitions import skill_hierarchy
                if hierarchy_dict is None:
                    hierarchy_dict = skill_hierarchy
        except ImportError:
            self.db_postgres.logger.error("Could not import skill hierarchy definitions")
            return False
            
        logger = self.db_postgres.logger
        cursor = self.db_postgres.get_cursor()
        if not cursor:
            logger.error("Failed to get a database cursor. Exiting.")
            return False
        
        try:
            logger.info("Starting COMPLETE hierarchy relationships population...")
            relationships_added = 0
            relationships_skipped = 0
            hierarchy_skills_created = 0
            job_skills_processed = 0
            
            # PHASE 1: Insert ALL hierarchy relationships, creating missing skills as needed
            logger.info("Phase 1: Processing ALL hierarchy relationships...")
            
            # Keep track of all skills we create/find
            all_skill_ids = {}
            
            # First, get existing skills from database
            cursor.execute("SELECT id, name FROM skill")
            existing_skills = cursor.fetchall()
            for skill in existing_skills:
                all_skill_ids[skill['name'].lower()] = skill['id']
            
            logger.info(f"Found {len(all_skill_ids)} existing skills in database")
            
            # Process every relationship in the hierarchy
            for parent_name, children_data in hierarchy_dict.items():
                # Ensure parent skill exists
                parent_id = all_skill_ids.get(parent_name.lower())
                if not parent_id:
                    # Create parent skill from hierarchy
                    cursor.execute("""
                        INSERT INTO skill (name, description, metadata)
                        VALUES (%s, %s, %s)
                        RETURNING id
                    """, (
                        parent_name,
                        f"Skill from hierarchy: {parent_name}",
                        json.dumps({
                            "source": "skills_hierarchy_definitions",
                            "extracted_date": "2024",
                            "skill_type": "hierarchy_defined",
                            "in_job_data": False,
                            "is_parent_skill": True,
                            "note": "Created from hierarchy, not found in job extractions"
                        })
                    ))
                    parent_id = cursor.fetchone()['id']
                    all_skill_ids[parent_name.lower()] = parent_id
                    hierarchy_skills_created += 1
                    logger.info(f"Created hierarchy parent skill: {parent_name}")
                
                # Handle both weighted format and simple format
                if use_weights and isinstance(children_data, list) and len(children_data) > 0 and isinstance(children_data[0], dict):
                    children_relationships = children_data
                else:
                    children_relationships = [{"child": child, "weight": 1.0} for child in children_data]
                
                # Process each child relationship
                for relationship in children_relationships:
                    child_name = relationship["child"]
                    weight = relationship.get("weight", 1.0)
                    
                    # Ensure child skill exists
                    child_id = all_skill_ids.get(child_name.lower())
                    if not child_id:
                        # Create child skill from hierarchy
                        cursor.execute("""
                            INSERT INTO skill (name, description, metadata)
                            VALUES (%s, %s, %s)
                            RETURNING id
                        """, (
                            child_name,
                            f"Skill from hierarchy: {child_name}",
                            json.dumps({
                                "source": "skills_hierarchy_definitions",
                                "extracted_date": "2024",
                                "skill_type": "hierarchy_defined",
                                "in_job_data": False,
                                "note": "Created from hierarchy, not found in job extractions"
                            })
                        ))
                        child_id = cursor.fetchone()['id']
                        all_skill_ids[child_name.lower()] = child_id
                        hierarchy_skills_created += 1
                        logger.info(f"Created hierarchy child skill: {child_name}")
                    
                    # Check for existing relationship to prevent duplicates
                    cursor.execute(
                        "SELECT id FROM skillrelationship WHERE parentid = %s AND childid = %s",
                        (parent_id, child_id)
                    )
                    if cursor.fetchone() is None:
                        cursor.execute(
                            """INSERT INTO skillrelationship (parentid, childid, weight, metadata) 
                            VALUES (%s, %s, %s, %s)""",
                            (parent_id, child_id, weight, json.dumps({
                                "relationship_type": "hierarchy",
                                "created_from": "skill_hierarchy_definitions",
                                "parent_skill": parent_name,
                                "child_skill": child_name,
                                "skill_weight": weight,
                                "weight_explanation": f"Weight {weight} represents importance of {child_name} as a component of {parent_name}",
                                "source": "complete_hierarchy_import"
                            }))
                        )
                        
                        logger.debug(f"Inserted hierarchy relationship: {parent_name} -> {child_name} (weight: {weight})")
                        relationships_added += 1
                    else:
                        logger.debug(f"Hierarchy relationship already exists: '{parent_name}' -> '{child_name}'")
                        relationships_skipped += 1
            
            # PHASE 2: Update metadata for job-extracted skills
            logger.info("Phase 2: Updating metadata for job-extracted skills...")
            
            for job_skill_name, job_skill_id in skill_name_to_id.items():
                if job_skill_name.lower() in all_skill_ids:
                    # This skill exists in both job data and hierarchy - update metadata
                    cursor.execute("""
                        UPDATE skill 
                        SET metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb
                        WHERE id = %s
                    """, (
                        json.dumps({
                            "in_job_data": True,
                            "job_extracted": True,
                            "found_in_hierarchy": True
                        }),
                        all_skill_ids[job_skill_name.lower()]
                    ))
                    job_skills_processed += 1
                else:
                    # This is a job skill not in our hierarchy - it should already exist from job processing
                    # Just update its metadata to indicate it's not in hierarchy
                    cursor.execute("""
                        UPDATE skill 
                        SET metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb
                        WHERE id = %s
                    """, (
                        json.dumps({
                            "in_job_data": True,
                            "job_extracted": True,
                            "found_in_hierarchy": False,
                            "note": "Skill extracted from jobs but not defined in skills hierarchy"
                        }),
                        job_skill_id
                    ))
                    job_skills_processed += 1
                    logger.info(f"Job skill not in hierarchy: {job_skill_name}")
            
            logger.info(f"COMPLETE skill relationships population finished!")
            logger.info(f"Hierarchy skills created: {hierarchy_skills_created}")
            logger.info(f"Relationships added: {relationships_added}")
            logger.info(f"Relationships skipped (already existed): {relationships_skipped}")
            logger.info(f"Job skills processed: {job_skills_processed}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error populating complete skill relationships: {e}")
            return False

    def get_skill_coverage_analysis(self):
        """Analyze the coverage between hierarchy and job-extracted skills"""
        cursor = self.db_postgres.get_cursor()
        
        # Get skills by source
        cursor.execute("""
            SELECT 
                name,
                metadata->>'source' as source,
                COALESCE(metadata->>'in_job_data', 'false')::boolean as in_job_data,
                COALESCE(metadata->>'found_in_hierarchy', 'false')::boolean as found_in_hierarchy
            FROM skill
            ORDER BY name
        """)
        skills = cursor.fetchall()
        
        hierarchy_only = []
        job_only = []
        both = []
        
        for skill in skills:
            if skill['in_job_data'] and skill['found_in_hierarchy']:
                both.append(skill['name'])
            elif skill['in_job_data'] and not skill['found_in_hierarchy']:
                job_only.append(skill['name'])
            elif not skill['in_job_data']:
                hierarchy_only.append(skill['name'])
        
        self.db_postgres.logger.info(f"\n=== SKILL COVERAGE ANALYSIS ===")
        self.db_postgres.logger.info(f"Skills in BOTH hierarchy and jobs: {len(both)}")
        self.db_postgres.logger.info(f"Skills ONLY in hierarchy: {len(hierarchy_only)}")
        self.db_postgres.logger.info(f"Skills ONLY in jobs: {len(job_only)}")
        self.db_postgres.logger.info(f"Total skills: {len(skills)}")
        
        if both:
            self.db_postgres.logger.info(f"\nSample skills in both: {both[:10]}")
        if hierarchy_only:
            self.db_postgres.logger.info(f"\nSample hierarchy-only skills: {hierarchy_only[:10]}")
        if job_only:
            self.db_postgres.logger.info(f"\nSample job-only skills: {job_only[:10]}")
        
        return {
            'both': both,
            'hierarchy_only': hierarchy_only,
            'job_only': job_only,
            'total': len(skills)
        }

    def process_all_jobs(self, json_file_path):
        """Process all jobs from JSON file and populate ALL hierarchy relationships"""
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
            
            # Clear competency tracking at start
            self.competency_to_skillgroups.clear()
            
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
            
            # Finalize competencies with skillgroup mappings
            self.db_postgres.logger.info("Finalizing competencies...")
            if self.insert_competencies():
                self.db_postgres.logger.info("Competencies finalized successfully!")
            else:
                self.db_postgres.logger.error("Failed to finalize competencies")
            
            # Now populate ALL skill relationships from hierarchy + job skills
            self.db_postgres.logger.info(f"Populating COMPLETE skill relationships...")
            self.db_postgres.logger.info(f"Job-extracted skills: {len(all_skills_to_ids)}")
            
            if self.populate_skill_relationships(all_skills_to_ids):
                self.db_postgres.logger.info("Complete skill relationships populated successfully!")
                
                # Analyze coverage
                coverage = self.get_skill_coverage_analysis()
                
            else:
                self.db_postgres.logger.error("Failed to populate skill relationships")
            
            return processed_count
            
        except Exception as e:
            self.db_postgres.logger.error(f"Error processing jobs file: {e}")
            return 0

    def analyze_skill_weights(self):
        """Analyze weight distribution and importance"""
        cursor = self.db_postgres.get_cursor()
        
        cursor.execute("""
            SELECT 
                p.name as parent_skill,
                c.name as child_skill,
                sr.weight,
                sr.metadata
            FROM skillrelationship sr
            JOIN skill p ON sr.parentid = p.id
            JOIN skill c ON sr.childid = c.id
            ORDER BY sr.weight DESC
        """)
        
        relationships = cursor.fetchall()
        
        self.db_postgres.logger.info(f"\n=== SKILL WEIGHT ANALYSIS ===")
        
        if not relationships:
            self.db_postgres.logger.info("No relationships found")
            return
        
        # Calculate statistics
        weights = [rel['weight'] for rel in relationships]
        avg_weight = sum(weights) / len(weights)
        max_weight = max(weights)
        min_weight = min(weights)
        
        self.db_postgres.logger.info(f"Total relationships: {len(relationships)}")
        self.db_postgres.logger.info(f"Average weight: {avg_weight:.2f}")
        self.db_postgres.logger.info(f"Weight range: {min_weight} - {max_weight}")
        
        # Count by weight categories
        critical = len([w for w in weights if w >= 0.8])
        important = len([w for w in weights if 0.7 <= w < 0.8])
        moderate = len([w for w in weights if 0.6 <= w < 0.7])
        low = len([w for w in weights if w < 0.6])
        
        self.db_postgres.logger.info(f"Weight distribution:")
        self.db_postgres.logger.info(f"  Critical (≥0.8): {critical} relationships")
        self.db_postgres.logger.info(f"  Important (0.7-0.79): {important} relationships")
        self.db_postgres.logger.info(f"  Moderate (0.6-0.69): {moderate} relationships")
        self.db_postgres.logger.info(f"  Low (<0.6): {low} relationships")
        
        # Show top 15 most important relationships
        self.db_postgres.logger.info(f"\nTop 15 Most Critical Skills (highest weights):")
        for i, rel in enumerate(relationships[:15], 1):
            self.db_postgres.logger.info(
                f"{i:2d}. {rel['parent_skill']} → {rel['child_skill']} [{rel['weight']}]"
            )
        
        return relationships

    def get_skill_relationships_summary(self, limit=50):
        """Get a summary of skill relationships for verification"""
        cursor = self.db_postgres.get_cursor()
        
        cursor.execute("""
            SELECT 
                sr.id,
                p.name as parent_skill,
                c.name as child_skill,
                sr.weight,
                sr.metadata
            FROM skillrelationship sr
            JOIN skill p ON sr.parentid = p.id
            JOIN skill c ON sr.childid = c.id
            ORDER BY sr.weight DESC, p.name, c.name
            LIMIT %s
        """, (limit,))
        
        relationships = cursor.fetchall()
        
        self.db_postgres.logger.info(f"Found {len(relationships)} skill relationships:")
        
        # Group by weight ranges for better understanding
        weight_groups = {
            "Critical (≥0.8)": [],
            "Important (0.7-0.79)": [],
            "Moderate (0.6-0.69)": [],
            "Low (<0.6)": []
        }
        
        for rel in relationships:
            weight = rel['weight']
            
            # Categorize by weight
            if weight >= 0.8:
                category = "Critical (≥0.8)"
            elif weight >= 0.7:
                category = "Important (0.7-0.79)"
            elif weight >= 0.6:
                category = "Moderate (0.6-0.69)"
            else:
                category = "Low (<0.6)"
            
            weight_groups[category].append(f"  {rel['parent_skill']} → {rel['child_skill']} [{weight}]")
        
        for category, relationships_list in weight_groups.items():
            if relationships_list:
                self.db_postgres.logger.info(f"\n{category}:")
                for rel_str in relationships_list[:10]:  # Show max 10 per category
                    self.db_postgres.logger.info(rel_str)
                if len(relationships_list) > 10:
                    self.db_postgres.logger.info(f"  ... and {len(relationships_list) - 10} more")
        
        return relationships

    def get_competency_analysis(self):
        """Get analysis of competency-skillgroup mappings"""
        cursor = self.db_postgres.get_cursor()
        
        cursor.execute("""
            SELECT 
                name,
                skillgroup,
                skillgroupweight,
                array_length(skillgroup, 1) as skillgroup_count,
                metadata
            FROM competency
            ORDER BY array_length(skillgroup, 1) DESC, name
        """)
        
        competencies = cursor.fetchall()
        
        self.db_postgres.logger.info(f"\n=== COMPETENCY ANALYSIS ===")
        self.db_postgres.logger.info(f"Total competencies: {len(competencies)}")
        
        # Group by skillgroup count
        single_skillgroup = []
        multiple_skillgroups = []
        
        for comp in competencies:
            if comp['skillgroup_count'] == 1:
                single_skillgroup.append(comp)
            elif comp['skillgroup_count'] > 1:
                multiple_skillgroups.append(comp)
        
        self.db_postgres.logger.info(f"Competencies in single skillgroup: {len(single_skillgroup)}")
        self.db_postgres.logger.info(f"Competencies in multiple skillgroups: {len(multiple_skillgroups)}")
        
        if multiple_skillgroups:
            self.db_postgres.logger.info(f"\nCompetencies with multiple skillgroups (showing first 10):")
            for comp in multiple_skillgroups[:10]:
                weights_info = "with weights" if comp['skillgroupweight'] else "no weights"
                self.db_postgres.logger.info(f"  {comp['name']}: {comp['skillgroup_count']} skillgroups ({weights_info})")
                if comp['skillgroupweight']:
                    weights = json.loads(comp['skillgroupweight'])
                    self.db_postgres.logger.info(f"    Weights: {weights}")
        
        return {
            'total': len(competencies),
            'single_skillgroup': len(single_skillgroup),
            'multiple_skillgroups': len(multiple_skillgroups)
        }

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
        
        cursor.execute("SELECT COUNT(*) as count FROM competency")
        competency_count = cursor.fetchone()['count']
        
        return {
            'jobs': job_count,
            'skills': skill_count,
            'skillgroups': skillgroup_count,
            'skill_relationships': relationship_count,
            'competencies': competency_count
        }

    def close_connection(self):
        """Close database connection"""
        self.db_postgres.close()
        
    def clear_and_restart(self):
        """Clear all job-related data and restart fresh"""
        cursor = self.db_postgres.get_cursor()
    
        # List all tables to clear (add any new tables here)
        tables_to_clear = [
            'frequency',
            'competency',
            'job',
            'skillgroup',
            'skillrelationship',
            'skill',
            # Add more tables here if needed
        ]
    
        # Clear tables in reverse dependency order
        for table in tables_to_clear:
            cursor.execute(f"DELETE FROM {table}")
    
        # Reset sequences to start from 1 (check if they exist first)
        sequences_to_reset = [
            'skill_id_seq',
            'skillgroup_id_seq', 
            'skillrelationship_id_seq',
            'competency_id_seq'
        ]
    
        for sequence in sequences_to_reset:
            try:
                cursor.execute(f"ALTER SEQUENCE {sequence} RESTART WITH 1")
                self.db_postgres.logger.debug(f"Reset sequence: {sequence}")
            except Exception as e:
                self.db_postgres.logger.debug(f"Could not reset sequence {sequence}: {e}")
    
        # Clear competency tracking
        self.competency_to_skillgroups.clear()
    
        self.db_postgres.logger.info("Cleared all job, competency, skillgroup, skill, frequency, and relationship data and reset ID sequences")
if __name__ == "__main__":
    processor = JobProcessor()
    processor.clear_and_restart()  # Uncomment to reset all data and IDs
    
    print("Starting COMPLETE HIERARCHY job processing pipeline...")
    print("This will insert ALL relationships from skills_hierarchy_definitions.py")
    print("Creating placeholder skills as needed, then marking job-extracted skills.")
    print()
    
    processed_count = processor.process_all_jobs("../jobs.json")
    
    summary = processor.get_processing_summary()
    print(f"\nProcessing completed!")
    print(f"Processed {processed_count} jobs")
    print(f"Database summary:")
    print(f"  - Jobs: {summary['jobs']}")
    print(f"  - Skills: {summary['skills']}")
    print(f"  - Skill Groups: {summary['skillgroups']}")
    print(f"  - Skill Relationships: {summary['skill_relationships']}")
    print(f"  - Competencies: {summary['competencies']}")
    
    # Show competency analysis
    if summary['competencies'] > 0:
        print(f"\n" + "="*50)
        print("COMPETENCY ANALYSIS")
        print("="*50)
        processor.get_competency_analysis()
    
    # Show weight analysis
    if summary['skill_relationships'] > 0:
        print(f"\n" + "="*50)
        print("COMPLETE HIERARCHY ANALYSIS")
        print("="*50)
        
        # Overall weight analysis
        processor.analyze_skill_weights()
        
        print(f"\n" + "="*50)
        print("SKILL RELATIONSHIPS BY WEIGHT CATEGORIES")
        print("="*50)
        processor.get_skill_relationships_summary(limit=40)
        
        print(f"\n" + "="*50)
        print("SKILL COVERAGE ANALYSIS")
        print("="*50)
        coverage = processor.get_skill_coverage_analysis()
        
        print(f"\n" + "="*50)
        print("VERIFICATION QUERIES")
        print("="*50)
        
        cursor = processor.db_postgres.get_cursor()
        
        # Show some examples of each type
        print("\nExample skills ONLY in hierarchy (not in jobs):")
        cursor.execute("""
            SELECT name FROM skill 
            WHERE metadata->>'in_job_data' = 'false' 
            LIMIT 5
        """)
        for row in cursor.fetchall():
            print(f"  - {row['name']}")
        
        print("\nExample skills in BOTH hierarchy and jobs:")
        cursor.execute("""
            SELECT name FROM skill 
            WHERE metadata->>'in_job_data' = 'true' 
            AND metadata->>'found_in_hierarchy' = 'true'
            LIMIT 5
        """)
        for row in cursor.fetchall():
            print(f"  - {row['name']}")
        
        print("\nExample skills ONLY in jobs (not in hierarchy):")
        cursor.execute("""
            SELECT name FROM skill 
            WHERE metadata->>'in_job_data' = 'true' 
            AND metadata->>'found_in_hierarchy' = 'false'
            LIMIT 5
        """)
        for row in cursor.fetchall():
            print(f"  - {row['name']}")
        
        # Show total relationships by source
        cursor.execute("""
            SELECT 
                metadata->>'source' as source,
                COUNT(*) as count
            FROM skillrelationship 
            GROUP BY metadata->>'source'
        """)
        print(f"\nSkill relationships by source:")
        for row in cursor.fetchall():
            print(f"  - {row['source']}: {row['count']} relationships")
    
    processor.close_connection()
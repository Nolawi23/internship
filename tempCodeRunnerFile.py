    def setup_database():
        """Setup complete database with tables"""
        try:
            pipeline = PostgresSkillsPipeline()
            if pipeline.create_tables():
                print("Database setup completed successfully!")
                return pipeline
            else:
                print("Failed to create tables")
                pipeline.close()
                return None
        except Exception as e:
            print(f"Failed to setup database: {e}")
            return None

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

    def create_skillgroup(self, skills_list, job_id):
        """Create skillgroup for given skills"""
        cursor = self.get_cursor()
        
        skillgroup_name = f"job_{job_id}_skills"
        
        cursor.execute("""
            INSERT INTO skillgroup (name, description, metadata)
            VALUES (%s, %s, %s)
            RETURNING id
        """, (
            skillgroup_name,
            f"Skills for job {job_id}",
            {"skills": skills_list, "job_id": job_id}
        ))
        
        return cursor.fetchone()['id']

    def insert_job(self, job_data):
        """Insert job with skillgroup"""
        cursor = self.get_cursor()
        
        job_id = job_data.get('id')
        if not job_id:
            return False
        
        # Check if job already exists
        cursor.execute("SELECT id FROM job WHERE id = %s", (job_id,))
        if cursor.fetchone():
            return True
        
        # Extract skills and create skillgroup
        skills = self.extract_skills_from_job(job_data)
        skillgroup_id = self.create_skillgroup(skills, job_id)
        
        # Extract SFIA levels
        sfia_data = {}
        attributes = job_data.get('attributes', {})
        if isinstance(attributes, dict) and 'attributes' in attributes:
            attributes = attributes['attributes']
        
        competencies = attributes.get('competencies', [])
        for competency in competencies:
            if isinstance(competency, dict):
                comp_name = competency.get('name', '')
                sfia_level = competency.get('sfia_level', 1)
                if comp_name:
                    sfia_data[comp_name] = sfia_level
        
        # Insert job
        cursor.execute("""
            INSERT INTO job (id, sfialevel, skillgroup)
            VALUES (%s, %s, %s)
        """, (job_id, sfia_data, skillgroup_id))
        
        return True

    def process_jobs_file(self, json_file_path):
        """Process jobs from JSON file"""
        import json
        
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
            for job in jobs:
                if isinstance(job, dict) and self.insert_job(job):
                    processed_count += 1
            
            return processed_count
            
        except Exception as e:
            self.logger.error(f"Error: {e}")
            return 0
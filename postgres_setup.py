import psycopg2
import psycopg2.extras
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import logging
from typing import Optional

DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = '10academy'
DB_USER = 'postgres'
DB_PASS = 'Nol20050723#'

class PostgresSkillsPipeline:
    def __init__(self):
        """Initialize PostgreSQL connection for skills pipeline"""
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Connect to PostgreSQL using constants
        try:
            self.conn = psycopg2.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASS,
                port=DB_PORT
            )
            self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            self.logger.info("Connected to PostgreSQL successfully")
        except Exception as e:
            self.logger.error(f"Failed to connect to PostgreSQL: {e}")
            self.conn = None
            raise
    
    def create_tables(self):
        """Create all required tables according to schema"""
        if not self.conn:
            self.logger.error("No database connection")
            return False
        
        try:
            cursor = self.conn.cursor()
            
            # Create ENUM types first (check if exists before creating)
            cursor.execute("""
                DO $$ BEGIN
                    CREATE TYPE evidence_type AS ENUM (
                        'assignment', 'assessment', 'observation', 'portfolio'
                    );
                EXCEPTION
                    WHEN duplicate_object THEN null;
                END $$;
            """)
            
            # Create tables in correct order (respecting foreign keys)
            
            # 1. Trainee table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS trainee (
                    id SERIAL PRIMARY KEY,
                    traineid INTEGER,
                    email VARCHAR(255),
                    role VARCHAR(255)
                )
            """)
            
            # 2. Batch table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS batch (
                    id SERIAL PRIMARY KEY,
                    batchlink VARCHAR(255)
                )
            """)
            
            # 3. Assignment table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS assignment (
                    id SERIAL PRIMARY KEY,
                    traineid INTEGER,
                    assignmentcategory INTEGER
                )
            """)
            
            # 4. AssignmentCategory table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS assignmentcategory (
                    id SERIAL PRIMARY KEY,
                    otherfields VARCHAR(255),
                    skillgroup INTEGER,
                    summary VARCHAR(255)
                )
            """)
            
            # 5. Assignment_Response table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS assignment_response (
                    id SERIAL PRIMARY KEY,
                    assignment INTEGER REFERENCES assignment(id)
                )
            """)
            
            # 6. Challenge table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS challenge (
                    id SERIAL PRIMARY KEY,
                    otherfields VARCHAR(255),
                    assignmentcategoryid INTEGER,
                    summary VARCHAR(255),
                    challengelevel VARCHAR(255)
                )
            """)
            
            # 7. Competency table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS competency (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255),
                    skillgroup INTEGER,
                    skillweight JSONB,
                    metadata JSONB,
                    evidence INTEGER
                )
            """)
            
            # 8. SkillGroup table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS skillgroup (
                    id SERIAL PRIMARY KEY,
                    grouptype evidence_type,
                    name VARCHAR(255),
                    description VARCHAR(255),
                    skillids INTEGER,
                    weight INTEGER,
                    metadata JSONB,
                    scoretosfiathreshold JSONB
                )
            """)
            
            # 9. SkillRelationship table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS skillrelationship (
                    id SERIAL PRIMARY KEY,
                    parentid INTEGER,
                    childid INTEGER,
                    weight INTEGER,
                    metadata JSONB
                )
            """)
            
            # 10. Skill table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS skill (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255),
                    description VARCHAR(255),
                    metadata JSONB
                )
            """)
            
            # 11. Evidence table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS evidence (
                    id SERIAL PRIMARY KEY,
                    traineid INTEGER,
                    sfialevel INTEGER,
                    weightedscore FLOAT,
                    score INTEGER,
                    feedback VARCHAR(1000),
                    skillgroup INTEGER,
                    evidencetype evidence_type,
                    evidencewriter VARCHAR(255),
                    metadata JSONB
                )
            """)
            
            # 12. Job table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS job (
                    id SERIAL PRIMARY KEY,
                    sfialevel JSONB,
                    skillgroup INTEGER
                )
            """)
            
            # Create indexes for performance
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_trainee_email ON trainee(email)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_assignment_traineid ON assignment(traineid)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_assignment_category ON assignment(assignmentcategory)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_skill_name ON skill(name)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_skillgroup_name ON skillgroup(name)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_competency_name ON competency(name)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_skillrel_parent ON skillrelationship(parentid)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_skillrel_child ON skillrelationship(childid)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_evidence_traineid ON evidence(traineid)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_evidence_skillgroup ON evidence(skillgroup)")
            
            self.logger.info("All tables created successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error creating tables: {e}")
            return False
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            self.logger.info("Database connection closed")
    
    def get_cursor(self):
        """Get database cursor"""
        if self.conn:
            return self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        return None

# Usage function
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

# Example usage
# Example usage
if __name__ == "__main__":
    # Setup database using constants defined at top of file
    db_pipeline = setup_database()
    
    if db_pipeline:
        print("Database setup completed!")
        
        # Process jobs from JSON file
        processed_count = db_pipeline.process_jobs_file("jobs.json")
        print(f"Processed {processed_count} jobs")
        
        # Close connection
        db_pipeline.close()
    else:
        print("Database setup failed")

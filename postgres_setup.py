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
        # Create tables
        try:
            if not self.create_tables():
                self.logger.error("Failed to create tables")
                raise Exception("Table creation failed")  # ✅ Raise exception instead
            else:
                self.logger.info("Database setup completed successfully!")
        except Exception as e:
            self.logger.error(f"Failed to setup database: {e}")
            if self.conn:
                self.conn.close()  
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
                    skillids INTEGER[],
                    skillweight JSONB,
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
                    weight FLOAT,
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
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS frequency (
                    skill_id INTEGER PRIMARY KEY REFERENCES skill(id) ON DELETE CASCADE,  -- ✅ Direct primary key
                    name VARCHAR(255) NOT NULL,                                          -- ✅ Skill name included
                    direct_frequency INTEGER DEFAULT 0,
                    total_frequency INTEGER DEFAULT 0,
                    job_count INTEGER DEFAULT 0,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_frequency_name ON frequency(name)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_frequency_total_freq ON frequency(total_frequency)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_frequency_direct_freq ON frequency(direct_frequency)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_frequency_job_count ON frequency(job_count)")

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
        if self.conn and not self.conn.closed:  # ✅ Check if connection is alive
            return self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        else:
            self.logger.error("Database connection is not available or closed")
            return None

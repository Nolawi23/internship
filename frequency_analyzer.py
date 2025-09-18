from collections import defaultdict
from postgres_setup import PostgresSkillsPipeline


class SkillFrequencyAnalyzer:
    def __init__(self):
        self.db = PostgresSkillsPipeline()
        self.parent_relationships = {}  # child_id -> [parent_ids]
        self._load_relationships()
    
    def _load_relationships(self):
        """Load parent-child relationships for hierarchy updates"""
        cursor = self.db.get_cursor()
        cursor.execute("SELECT parentid, childid FROM skillrelationship")
        
        for rel in cursor.fetchall():
            child_id = rel['childid']
            parent_id = rel['parentid']
            
            if child_id not in self.parent_relationships:
                self.parent_relationships[child_id] = []
            self.parent_relationships[child_id].append(parent_id)
        
        print(f"Loaded {len(self.parent_relationships)} skill relationships")
    
    def calculate_frequencies(self):
        """Calculate skill frequencies from job data - ONLY for job-extracted skills"""
        cursor = self.db.get_cursor()
        
        # Clear existing frequency data
        cursor.execute("DELETE FROM frequency")
        
        # Step 1: Insert direct skill frequencies - ONLY for skills extracted from jobs
        cursor.execute("""
            INSERT INTO frequency (skill_id, name, direct_frequency, total_frequency, job_count)
            SELECT 
                skill_mentions.skill_id,
                s.name,
                COUNT(*) as direct_frequency,
                COUNT(*) as total_frequency,
                COUNT(DISTINCT skill_mentions.job_id) as job_count
            FROM (
                SELECT 
                    UNNEST(sg.skillids) as skill_id,
                    j.id as job_id
                FROM job j
                JOIN skillgroup sg ON j.skillgroup = sg.id
                WHERE sg.skillids IS NOT NULL
            ) skill_mentions
            JOIN skill s ON skill_mentions.skill_id = s.id
            WHERE s.description LIKE '%extracted from job%'  -- Only job-extracted skills
            GROUP BY skill_mentions.skill_id, s.name
        """)
        
        direct_skills_count = cursor.rowcount
        print(f"✅ Inserted {direct_skills_count} job-extracted skill frequencies")
        
        # Step 2: Calculate parent frequencies using recursive approach
        self._calculate_parent_frequencies()
        
        self.db.conn.commit()
        return direct_skills_count
    
    def _calculate_parent_frequencies(self):
        """Calculate frequencies for parent skills based on their JOB-EXTRACTED children only"""
        cursor = self.db.get_cursor()
        
        # Use recursive SQL to calculate parent frequencies - only from job-extracted skills
        cursor.execute("""
            WITH RECURSIVE parent_frequencies AS (
                -- Base case: job-extracted skills only (those that appear in jobs)
                SELECT 
                    f.skill_id,
                    f.total_frequency,
                    ARRAY[f.skill_id] as skill_path,
                    f.skill_id as original_skill
                FROM frequency f
                JOIN skill s ON f.skill_id = s.id
                WHERE s.description LIKE '%extracted from job%'  -- Only job-extracted skills
                
                UNION
                
                -- Recursive case: propagate to parents
                SELECT 
                    sr.parentid as skill_id,
                    pf.total_frequency,
                    pf.skill_path || sr.parentid as skill_path,
                    pf.original_skill
                FROM parent_frequencies pf
                JOIN skillrelationship sr ON pf.skill_id = sr.childid
                WHERE NOT sr.parentid = ANY(pf.skill_path)  -- Prevent cycles
            ),
            parent_aggregates AS (
                SELECT 
                    pf.skill_id,
                    SUM(pf.total_frequency) as total_freq,
                    COUNT(DISTINCT pf.original_skill) as unique_children,
                    array_agg(DISTINCT pf.original_skill) as child_skills
                FROM parent_frequencies pf
                WHERE pf.skill_id != pf.original_skill  -- Only parents, not the original skills
                GROUP BY pf.skill_id
            )
            INSERT INTO frequency (skill_id, name, direct_frequency, total_frequency, job_count)
            SELECT 
                pa.skill_id,
                parent_skill.name,
                0 as direct_frequency,  -- Parents don't have direct mentions
                pa.total_freq as total_frequency,
                (
                    -- Count unique jobs where any job-extracted child skill appears
                    SELECT COUNT(DISTINCT job_id)
                    FROM (
                        SELECT UNNEST(sg.skillids) as skill_id, j.id as job_id
                        FROM job j
                        JOIN skillgroup sg ON j.skillgroup = sg.id
                        WHERE sg.skillids IS NOT NULL
                    ) all_mentions
                    JOIN skill child_skill ON all_mentions.skill_id = child_skill.id
                    WHERE all_mentions.skill_id = ANY(pa.child_skills)
                    AND child_skill.description LIKE '%extracted from job%'  -- Only job-extracted
                ) as job_count
            FROM parent_aggregates pa
            JOIN skill parent_skill ON pa.skill_id = parent_skill.id
            ON CONFLICT (skill_id)
            DO UPDATE SET
                name = EXCLUDED.name,
                total_frequency = EXCLUDED.total_frequency,
                job_count = EXCLUDED.job_count,
                last_updated = CURRENT_TIMESTAMP
        """)
        
        parent_count = cursor.rowcount
        print(f"✅ Calculated frequencies for {parent_count} parent skills (from job-extracted children only)")
    
    def get_top_skills(self, limit=20, by='total'):
        """Get top skills by frequency"""
        cursor = self.db.get_cursor()
        
        order_field = 'total_frequency' if by == 'total' else 'direct_frequency'
        
        cursor.execute(f"""
            SELECT 
                skill_id,
                name,
                direct_frequency,
                total_frequency,
                job_count
            FROM frequency
            ORDER BY {order_field} DESC, name
            LIMIT %s
        """, (limit,))
        
        return cursor.fetchall()
    
    def get_hierarchy_analysis(self, parent_skill_name):
        """Get frequency analysis for a skill hierarchy"""
        cursor = self.db.get_cursor()
        
        cursor.execute("""
            WITH RECURSIVE skill_tree AS (
                SELECT s.id, s.name, 0 as level
                FROM skill s 
                WHERE LOWER(s.name) = LOWER(%s)
                
                UNION ALL
                
                SELECT s.id, s.name, st.level + 1
                FROM skill s
                JOIN skillrelationship sr ON s.id = sr.childid
                JOIN skill_tree st ON sr.parentid = st.id
                WHERE st.level < 3
            )
            SELECT 
                st.name,
                st.level,
                COALESCE(f.direct_frequency, 0) as direct_frequency,
                COALESCE(f.total_frequency, 0) as total_frequency,
                COALESCE(f.job_count, 0) as job_count
            FROM skill_tree st
            LEFT JOIN frequency f ON st.id = f.skill_id
            ORDER BY st.level, f.total_frequency DESC NULLS LAST
        """, (parent_skill_name,))
        
        return cursor.fetchall()
    
    def get_summary(self):
        """Get frequency summary statistics"""
        cursor = self.db.get_cursor()
        
        cursor.execute("""
            SELECT 
                COUNT(*) as total_skills,
                SUM(direct_frequency) as total_direct,
                SUM(total_frequency) as total_with_hierarchy,
                AVG(direct_frequency) as avg_direct,
                MAX(total_frequency) as max_frequency
            FROM frequency
        """)
        
        return cursor.fetchone()
    
    def explain_frequency_calculation(self):
        """Explain how frequencies are calculated"""
        cursor = self.db.get_cursor()
        
        print("🔍 HOW FREQUENCY CALCULATION WORKS:")
        print("=" * 50)
        
        # Show job-extracted vs hierarchy skills
        cursor.execute("""
            SELECT 
                CASE 
                    WHEN description LIKE '%extracted from job%' THEN 'Job-extracted'
                    WHEN description LIKE '%from hierarchy%' THEN 'Hierarchy-only'
                    ELSE 'Other'
                END as skill_source,
                COUNT(*) as count
            FROM skill
            GROUP BY skill_source
        """)
        
        sources = cursor.fetchall()
        print("\n📊 Skill Sources in Database:")
        for source in sources:
            print(f"  {source['skill_source']}: {source['count']} skills")
        
        # Show how skills appear in jobs
        cursor.execute("""
            SELECT 
                j.id as job_id,
                array_length(sg.skillids, 1) as skills_count,
                sg.skillids[1:5] as first_5_skills  -- Show first 5 skill IDs
            FROM job j
            JOIN skillgroup sg ON j.skillgroup = sg.id
            WHERE sg.skillids IS NOT NULL
            LIMIT 3
        """)
        
        job_examples = cursor.fetchall()
        print(f"\n📋 Example Job-Skill Relationships:")
        for job in job_examples:
            print(f"  Job {job['job_id']}: {job['skills_count']} skills, first 5 IDs: {job['first_5_skills']}")
        
        # Show frequency calculation process
        print(f"\n🔢 Frequency Calculation Process:")
        print(f"  1. Extract skill IDs from job.skillgroup -> skillgroup.skillids arrays")
        print(f"  2. Filter to ONLY skills with description LIKE '%extracted from job%'")
        print(f"  3. Count how many times each job-extracted skill appears across all jobs")
        print(f"  4. Count how many different jobs each skill appears in")
        print(f"  5. Propagate frequencies UP the hierarchy to parent skills")
        
        # Show a real example
        cursor.execute("""
            SELECT 
                s.name,
                s.description,
                COUNT(DISTINCT j.id) as appears_in_jobs,
                COUNT(*) as total_mentions
            FROM (
                SELECT 
                    UNNEST(sg.skillids) as skill_id,
                    j.id as job_id
                FROM job j
                JOIN skillgroup sg ON j.skillgroup = sg.id
                WHERE sg.skillids IS NOT NULL
            ) mentions
            JOIN skill s ON mentions.skill_id = s.id
            WHERE s.description LIKE '%extracted from job%'
            GROUP BY s.id, s.name, s.description
            ORDER BY total_mentions DESC
            LIMIT 3
        """)
        
        examples = cursor.fetchall()
        print(f"\n📈 Real Frequency Examples (Job-extracted skills only):")
        for ex in examples:
            print(f"  '{ex['name']}': {ex['total_mentions']} mentions in {ex['appears_in_jobs']} jobs")
            print(f"    Source: {ex['description'][:50]}...")
    
    def get_skill_breakdown(self):
        """Get breakdown of skills by source and frequency"""
        cursor = self.db.get_cursor()
        
        cursor.execute("""
            SELECT 
                f.name,
                s.description,
                f.direct_frequency as direct_freq,
                f.total_frequency as total_freq,
                f.job_count,
                CASE 
                    WHEN s.description LIKE '%extracted from job%' THEN 'Job-extracted'
                    WHEN s.description LIKE '%from hierarchy%' THEN 'Hierarchy'
                    ELSE 'Other'
                END as source
            FROM frequency f
            JOIN skill s ON f.skill_id = s.id
            ORDER BY f.total_frequency DESC
            LIMIT 20
        """)
        
        return cursor.fetchall()
    
    def reset_frequencies(self):
        """Clear all frequency data"""
        cursor = self.db.get_cursor()
        cursor.execute("DELETE FROM frequency")
        self.db.conn.commit()
        print("🧹 Cleared all frequency data")
    
    def close(self):
        self.db.close()


# Simple usage example
if __name__ == "__main__":
    analyzer = SkillFrequencyAnalyzer()
    
    print("🔍 Understanding Skill Frequency Calculation")
    print("=" * 60)
    
    # First, explain how it works
    analyzer.explain_frequency_calculation()
    
    print(f"\n🔄 Calculating frequencies from database...")
    skills_processed = analyzer.calculate_frequencies()
    
    print(f"\n📊 Summary Statistics:")
    summary = analyzer.get_summary()
    print(f"  Total skills with frequencies: {summary['total_skills']}")
    print(f"  Total direct mentions: {summary['total_direct']}")
    print(f"  Average direct frequency: {summary['avg_direct']:.1f}")
    print(f"  Max total frequency: {summary['max_frequency']}")
    
    print(f"\n🏆 Top 10 Skills by Total Frequency:")
    top_total = analyzer.get_top_skills(10, by='total')
    for i, skill in enumerate(top_total, 1):
        print(f"  {i:2d}. {skill['name']}: {skill['total_frequency']} total ({skill['direct_frequency']} direct)")
    
    print(f"\n🎯 Top 10 Skills by Direct Mentions:")
    top_direct = analyzer.get_top_skills(10, by='direct')
    for i, skill in enumerate(top_direct, 1):
        print(f"  {i:2d}. {skill['name']}: {skill['direct_frequency']} mentions in {skill['job_count']} jobs")
    
    print(f"\n📋 Skill Source Breakdown (Top 10):")
    breakdown = analyzer.get_skill_breakdown()
    for skill in breakdown[:10]:
        print(f"  {skill['name']}: {skill['total_freq']} total, {skill['job_count']} jobs [{skill['source']}]")
    
    print(f"\n🌳 Programming Languages Hierarchy:")
    prog_hierarchy = analyzer.get_hierarchy_analysis("Programming Languages")
    for skill in prog_hierarchy:
        indent = "  " * (skill['level'] + 1)
        print(f"{indent}• {skill['name']}: {skill['total_frequency']} total ({skill['direct_frequency']} direct)")
    
    analyzer.close()
    print(f"\n✅ Analysis complete!")

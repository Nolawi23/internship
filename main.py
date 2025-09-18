#!/usr/bin/env python3
"""
Skills Extraction and Analysis System - Main Interface
=====================================================

This is the main entry point for the complete skills extraction and analysis system.
It provides a menu-driven interface for all operations.

Features:
- Job processing from JSON files
- Skill hierarchy creation
- Frequency analysis
- Comprehensive reporting
- Database management

Usage: python main.py
"""

import sys
import os
import json
from datetime import datetime
from job_processor import JobProcessor
from frequency_analyzer import SkillFrequencyAnalyzer
from postgres_setup import PostgresSkillsPipeline

class SkillsSystem:
    def __init__(self):
        """Initialize the complete skills system"""
        self.processor = None
        self.analyzer = None
        
    def test_database_connection(self):
        """Test if database connection works"""
        try:
            print("🔌 Testing database connection...")
            db = PostgresSkillsPipeline()
            cursor = db.get_cursor()
            if cursor:
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                db.close()
                print("✅ Database connection successful!")
                return True
            else:
                print("❌ Failed to get database cursor")
                return False
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            return False
    
    def show_main_menu(self):
        """Display the main menu"""
        print("\n" + "="*60)
        print("🎯 SKILLS EXTRACTION & ANALYSIS SYSTEM")
        print("="*60)
        print("1. 🔄 Complete Pipeline (Process jobs + Analyze frequencies)")
        print("2. 📊 Job Processing Only")
        print("3. 📈 Frequency Analysis Only")
        print("4. 📋 View System Status")
        print("5. 📊 Generate Reports")
        print("6. 🔍 Interactive Queries")
        print("7. 🧹 Database Management")
        print("8. ❓ Help & Information")
        print("9. 🚪 Exit")
        print("="*60)
    
    def complete_pipeline(self, jobs_file="jobs.json"):
        """Run the complete pipeline: job processing + frequency analysis"""
        print("\n🚀 STARTING COMPLETE PIPELINE")
        print("="*50)
        
        if not os.path.exists(jobs_file):
            print(f"❌ Jobs file not found: {jobs_file}")
            jobs_file = input("Enter path to jobs JSON file: ").strip()
            if not os.path.exists(jobs_file):
                print(f"❌ File still not found: {jobs_file}")
                return False
        
        try:
            # Step 1: Job Processing
            print(f"\n📊 Step 1: Processing jobs from {jobs_file}...")
            self.processor = JobProcessor()
            
            # Ask if user wants to clear existing data
            clear_data = input("Clear existing data and start fresh? (y/N): ").lower()
            if clear_data == 'y':
                print("🧹 Clearing existing data...")
                self.processor.clear_and_restart()
            
            processed_count = self.processor.process_all_jobs(jobs_file)
            
            if processed_count == 0:
                print("❌ No jobs were processed successfully")
                return False
            
            # Get processing summary
            summary = self.processor.get_processing_summary()
            print(f"\n✅ Job Processing Complete:")
            print(f"   📋 Jobs processed: {processed_count}")
            print(f"   🎯 Skills created: {summary['skills']}")
            print(f"   🔗 Relationships: {summary['skill_relationships']}")
            print(f"   📦 Skill groups: {summary['skillgroups']}")
            
            # Step 2: Frequency Analysis
            print(f"\n📈 Step 2: Analyzing skill frequencies...")
            self.analyzer = SkillFrequencyAnalyzer()
            skills_analyzed = self.analyzer.calculate_frequencies()
            
            freq_summary = self.analyzer.get_summary()
            print(f"\n✅ Frequency Analysis Complete:")
            print(f"   🎯 Skills analyzed: {skills_analyzed}")
            print(f"   📊 Total mentions: {freq_summary['total_direct']}")
            print(f"   📈 Max frequency: {freq_summary['max_frequency']}")
            print(f"   📊 Average frequency: {freq_summary['avg_direct']:.1f}")
            
            # Step 3: Show Key Results
            self.show_pipeline_results()
            
            return True
            
        except Exception as e:
            print(f"❌ Pipeline failed: {e}")
            import traceback
            traceback.print_exc()
            return False
        finally:
            if self.processor:
                self.processor.close_connection()
            if self.analyzer:
                self.analyzer.close()
    
    def show_pipeline_results(self):
        """Show key results from the pipeline"""
        if not self.analyzer:
            return
        
        print(f"\n🏆 TOP 15 MOST POPULAR SKILLS:")
        print("-" * 40)
        top_skills = self.analyzer.get_top_skills(15, by='total')
        for i, skill in enumerate(top_skills, 1):
            direct = skill['direct_frequency']
            total = skill['total_frequency']
            jobs = skill['job_count']
            print(f"{i:2d}. {skill['name'][:35]:35} | {total:3d} total ({direct:2d} direct) | {jobs:2d} jobs")
        
        print(f"\n🎯 TOP 10 SKILLS BY DIRECT JOB MENTIONS:")
        print("-" * 40)
        direct_skills = self.analyzer.get_top_skills(10, by='direct')
        for i, skill in enumerate(direct_skills, 1):
            direct = skill['direct_frequency']
            jobs = skill['job_count']
            avg = direct / jobs if jobs > 0 else 0
            print(f"{i:2d}. {skill['name'][:35]:35} | {direct:2d} mentions | {jobs:2d} jobs | {avg:.1f} avg")
        
        print(f"\n🌳 PROGRAMMING LANGUAGES HIERARCHY:")
        print("-" * 40)
        hierarchy = self.analyzer.get_hierarchy_analysis("Programming Languages")
        for skill in hierarchy[:12]:
            indent = "  " * skill['level']
            level_icon = ["🏠", "🌿", "📦", "🔧"][min(skill['level'], 3)]
            print(f"{indent}{level_icon} {skill['name']}: {skill['total_frequency']} total ({skill['direct_frequency']} direct)")
    
    def job_processing_only(self, jobs_file="jobs.json"):
        """Run job processing only"""
        print("\n📊 JOB PROCESSING ONLY")
        print("="*30)
        
        if not os.path.exists(jobs_file):
            jobs_file = input("Enter path to jobs JSON file: ").strip()
            if not os.path.exists(jobs_file):
                print(f"❌ File not found: {jobs_file}")
                return False
        
        try:
            self.processor = JobProcessor()
            
            clear_data = input("Clear existing data? (y/N): ").lower()
            if clear_data == 'y':
                self.processor.clear_and_restart()
            
            processed_count = self.processor.process_all_jobs(jobs_file)
            summary = self.processor.get_processing_summary()
            
            print(f"\n✅ Processing Complete:")
            print(f"   Jobs: {processed_count}")
            print(f"   Skills: {summary['skills']}")
            print(f"   Relationships: {summary['skill_relationships']}")
            
            return True
            
        except Exception as e:
            print(f"❌ Job processing failed: {e}")
            return False
        finally:
            if self.processor:
                self.processor.close_connection()
    
    def frequency_analysis_only(self):
        """Run frequency analysis on existing data"""
        print("\n📈 FREQUENCY ANALYSIS ONLY")
        print("="*35)
        
        try:
            self.analyzer = SkillFrequencyAnalyzer()
            skills_analyzed = self.analyzer.calculate_frequencies()
            
            if skills_analyzed == 0:
                print("❌ No skills found to analyze. Run job processing first.")
                return False
            
            freq_summary = self.analyzer.get_summary()
            print(f"\n✅ Analysis Complete:")
            print(f"   Skills analyzed: {skills_analyzed}")
            print(f"   Total mentions: {freq_summary['total_direct']}")
            print(f"   Max frequency: {freq_summary['max_frequency']}")
            
            # Show top skills
            print(f"\n🏆 Top 10 Skills:")
            top_skills = self.analyzer.get_top_skills(10)
            for i, skill in enumerate(top_skills, 1):
                print(f"  {i:2d}. {skill['name']}: {skill['total_frequency']} total")
            
            return True
            
        except Exception as e:
            print(f"❌ Frequency analysis failed: {e}")
            return False
        finally:
            if self.analyzer:
                self.analyzer.close()
    
    def view_system_status(self):
        """Show current system status"""
        print("\n📋 SYSTEM STATUS")
        print("="*25)
        
        try:
            db = PostgresSkillsPipeline()
            cursor = db.get_cursor()
            
            # Check table counts
            tables = ['job', 'skill', 'skillgroup', 'skillrelationship', 'frequency', 'competency']
            counts = {}
            
            for table in tables:
                try:
                    cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
                    counts[table] = cursor.fetchone()['count']
                except:
                    counts[table] = 0
            
            print(f"📊 Database Contents:")
            print(f"   Jobs: {counts['job']}")
            print(f"   Skills: {counts['skill']}")
            print(f"   Skill Groups: {counts['skillgroup']}")
            print(f"   Skill Relationships: {counts['skillrelationship']}")
            print(f"   Frequency Records: {counts['frequency']}")
            print(f"   Competencies: {counts['competency']}")
            
            # Check skill sources
            if counts['skill'] > 0:
                cursor.execute("""
                    SELECT 
                        CASE 
                            WHEN description LIKE '%extracted from job%' THEN 'Job-extracted'
                            WHEN description LIKE '%from hierarchy%' THEN 'Hierarchy'
                            ELSE 'Other'
                        END as source,
                        COUNT(*) as count
                    FROM skill
                    GROUP BY source
                """)
                sources = cursor.fetchall()
                print(f"\n🎯 Skill Sources:")
                for source in sources:
                    print(f"   {source['source']}: {source['count']}")
            
            # Check frequency status
            if counts['frequency'] > 0:
                cursor.execute("""
                    SELECT 
                        MAX(total_frequency) as max_freq,
                        AVG(direct_frequency) as avg_direct,
                        COUNT(*) as skills_with_freq
                    FROM frequency
                """)
                freq_stats = cursor.fetchone()
                print(f"\n📈 Frequency Analysis:")
                print(f"   Skills with frequencies: {freq_stats['skills_with_freq']}")
                print(f"   Max frequency: {freq_stats['max_freq']}")
                print(f"   Avg direct frequency: {freq_stats['avg_direct']:.1f}")
            
            db.close()
            
        except Exception as e:
            print(f"❌ Error checking status: {e}")
    
    def generate_reports(self):
        """Generate comprehensive reports"""
        print("\n📊 GENERATE REPORTS")
        print("="*25)
        print("1. 📈 Frequency Analysis Report")
        print("2. 🌳 Hierarchy Analysis Report") 
        print("3. 📋 Skills Coverage Report")
        print("4. 🔗 Relationships Summary")
        print("5. 📄 Export All Data")
        
        choice = input("\nSelect report type (1-5): ").strip()
        
        try:
            if choice == '1':
                self._generate_frequency_report()
            elif choice == '2':
                self._generate_hierarchy_report()
            elif choice == '3':
                self._generate_coverage_report()
            elif choice == '4':
                self._generate_relationships_report()
            elif choice == '5':
                self._export_all_data()
            else:
                print("❌ Invalid choice")
        except Exception as e:
            print(f"❌ Report generation failed: {e}")
    
    def _generate_frequency_report(self):
        """Generate detailed frequency analysis report"""
        try:
            analyzer = SkillFrequencyAnalyzer()
            
            # Get comprehensive data
            top_total = analyzer.get_top_skills(50, by='total')
            top_direct = analyzer.get_top_skills(30, by='direct')
            breakdown = analyzer.get_skill_breakdown()
            summary = analyzer.get_summary()
            
            # Generate report
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"frequency_report_{timestamp}.json"
            
            report = {
                'timestamp': datetime.now().isoformat(),
                'summary': dict(summary),
                'top_skills_by_total_frequency': [dict(skill) for skill in top_total],
                'top_skills_by_direct_mentions': [dict(skill) for skill in top_direct],
                'skill_source_breakdown': [dict(skill) for skill in breakdown]
            }
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            
            print(f"✅ Frequency report saved to: {filename}")
            analyzer.close()
            
        except Exception as e:
            print(f"❌ Failed to generate frequency report: {e}")
    
    def _generate_hierarchy_report(self):
        """Generate hierarchy analysis report"""
        try:
            analyzer = SkillFrequencyAnalyzer()
            
            # Analyze major hierarchies
            hierarchies = [
                "Programming Languages",
                "Machine Learning", 
                "Data Engineering",
                "Software Engineering Principles",
                "DevOps/MLOps"
            ]
            
            report = {
                'timestamp': datetime.now().isoformat(),
                'hierarchies': {}
            }
            
            for hierarchy_name in hierarchies:
                hierarchy_data = analyzer.get_hierarchy_analysis(hierarchy_name)
                if hierarchy_data:
                    report['hierarchies'][hierarchy_name] = [dict(skill) for skill in hierarchy_data]
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"hierarchy_report_{timestamp}.json"
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            
            print(f"✅ Hierarchy report saved to: {filename}")
            analyzer.close()
            
        except Exception as e:
            print(f"❌ Failed to generate hierarchy report: {e}")
    
    def _generate_coverage_report(self):
        """Generate skills coverage analysis report"""
        try:
            processor = JobProcessor()
            coverage = processor.get_skill_coverage_analysis()
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"coverage_report_{timestamp}.json"
            
            report = {
                'timestamp': datetime.now().isoformat(),
                'coverage_analysis': coverage
            }
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            
            print(f"✅ Coverage report saved to: {filename}")
            processor.close_connection()
            
        except Exception as e:
            print(f"❌ Failed to generate coverage report: {e}")
    
    def _generate_relationships_report(self):
        """Generate skill relationships summary"""
        try:
            processor = JobProcessor()
            relationships = processor.get_skill_relationships_summary(100)
            weights_analysis = processor.analyze_skill_weights()
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"relationships_report_{timestamp}.json"
            
            report = {
                'timestamp': datetime.now().isoformat(),
                'relationships_summary': [dict(rel) for rel in relationships],
                'weights_analysis': [dict(rel) for rel in weights_analysis] if weights_analysis else []
            }
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            
            print(f"✅ Relationships report saved to: {filename}")
            processor.close_connection()
            
        except Exception as e:
            print(f"❌ Failed to generate relationships report: {e}")
    
    def _export_all_data(self):
        """Export all system data"""
        try:
            db = PostgresSkillsPipeline()
            cursor = db.get_cursor()
            
            # Export all tables
            tables = ['skill', 'job', 'skillgroup', 'skillrelationship', 'frequency', 'competency']
            export_data = {'timestamp': datetime.now().isoformat()}
            
            for table in tables:
                cursor.execute(f"SELECT * FROM {table}")
                export_data[table] = [dict(row) for row in cursor.fetchall()]
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"complete_export_{timestamp}.json"
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(export_data, f, indent=2, ensure_ascii=False, default=str)
            
            print(f"✅ Complete data export saved to: {filename}")
            db.close()
            
        except Exception as e:
            print(f"❌ Failed to export data: {e}")
    
    def interactive_queries(self):
        """Interactive query interface"""
        print("\n🔍 INTERACTIVE QUERIES")
        print("="*30)
        print("1. 🔍 Search skills by name")
        print("2. 📊 Skills in specific hierarchy")
        print("3. 🎯 Jobs containing specific skill")
        print("4. 📈 Skills with frequency above threshold")
        print("5. 🔗 Parent-child relationships")
        
        choice = input("\nSelect query type (1-5): ").strip()
        
        try:
            if choice == '1':
                self._search_skills()
            elif choice == '2':
                self._query_hierarchy()
            elif choice == '3':
                self._query_jobs_with_skill()
            elif choice == '4':
                self._query_frequent_skills()
            elif choice == '5':
                self._query_relationships()
            else:
                print("❌ Invalid choice")
        except Exception as e:
            print(f"❌ Query failed: {e}")
    
    def _search_skills(self):
        """Search for skills by name"""
        search_term = input("Enter skill name to search: ").strip()
        if not search_term:
            return
        
        db = PostgresSkillsPipeline()
        cursor = db.get_cursor()
        
        cursor.execute("""
            SELECT name, description, 
                   COALESCE(f.total_frequency, 0) as frequency,
                   COALESCE(f.job_count, 0) as job_count
            FROM skill s
            LEFT JOIN frequency f ON s.id = f.skill_id
            WHERE LOWER(s.name) LIKE LOWER(%s)
            ORDER BY frequency DESC
        """, (f"%{search_term}%",))
        
        results = cursor.fetchall()
        
        print(f"\n🔍 Found {len(results)} skills matching '{search_term}':")
        for skill in results[:10]:
            print(f"  • {skill['name']}: {skill['frequency']} frequency, {skill['job_count']} jobs")
        
        db.close()
    
    def _query_hierarchy(self):
        """Query skills in specific hierarchy"""
        hierarchy_name = input("Enter hierarchy name: ").strip()
        if not hierarchy_name:
            return
        
        analyzer = SkillFrequencyAnalyzer()
        hierarchy = analyzer.get_hierarchy_analysis(hierarchy_name)
        
        if hierarchy:
            print(f"\n🌳 Skills in '{hierarchy_name}' hierarchy:")
            for skill in hierarchy:
                indent = "  " * skill['level']
                print(f"{indent}• {skill['name']}: {skill['total_frequency']} total, {skill['direct_frequency']} direct")
        else:
            print(f"❌ No hierarchy found for '{hierarchy_name}'")
        
        analyzer.close()
    
    def _query_jobs_with_skill(self):
        """Find jobs containing specific skill"""
        skill_name = input("Enter skill name: ").strip()
        if not skill_name:
            return
        
        db = PostgresSkillsPipeline()
        cursor = db.get_cursor()
        
        cursor.execute("""
            SELECT DISTINCT j.id as job_id
            FROM job j
            JOIN skillgroup sg ON j.skillgroup = sg.id
            JOIN skill s ON s.id = ANY(sg.skillids)
            WHERE LOWER(s.name) LIKE LOWER(%s)
            LIMIT 10
        """, (f"%{skill_name}%",))
        
        results = cursor.fetchall()
        
        print(f"\n📋 Found {len(results)} jobs with skill '{skill_name}':")
        for job in results:
            print(f"  • Job ID: {job['job_id']}")
        
        db.close()
    
    def _query_frequent_skills(self):
        """Query skills above frequency threshold"""
        try:
            threshold = int(input("Enter minimum frequency: ").strip())
        except ValueError:
            print("❌ Invalid number")
            return
        
        analyzer = SkillFrequencyAnalyzer()
        skills = analyzer.get_top_skills(100)
        
        frequent_skills = [s for s in skills if s['total_frequency'] >= threshold]
        
        print(f"\n📈 Found {len(frequent_skills)} skills with frequency >= {threshold}:")
        for skill in frequent_skills[:20]:
            print(f"  • {skill['name']}: {skill['total_frequency']} total, {skill['job_count']} jobs")
        
        analyzer.close()
    
    def _query_relationships(self):
        """Query parent-child relationships"""
        skill_name = input("Enter skill name to see relationships: ").strip()
        if not skill_name:
            return
        
        db = PostgresSkillsPipeline()
        cursor = db.get_cursor()
        
        # Find as parent
        cursor.execute("""
            SELECT c.name as child_name, sr.weight
            FROM skillrelationship sr
            JOIN skill p ON sr.parentid = p.id
            JOIN skill c ON sr.childid = c.id
            WHERE LOWER(p.name) LIKE LOWER(%s)
            ORDER BY sr.weight DESC
        """, (f"%{skill_name}%",))
        
        children = cursor.fetchall()
        
        # Find as child
        cursor.execute("""
            SELECT p.name as parent_name, sr.weight
            FROM skillrelationship sr
            JOIN skill p ON sr.parentid = p.id
            JOIN skill c ON sr.childid = c.id
            WHERE LOWER(c.name) LIKE LOWER(%s)
            ORDER BY sr.weight DESC
        """, (f"%{skill_name}%",))
        
        parents = cursor.fetchall()
        
        if children:
            print(f"\n👶 Children of '{skill_name}':")
            for child in children:
                print(f"  • {child['child_name']} (weight: {child['weight']})")
        
        if parents:
            print(f"\n👨 Parents of '{skill_name}':")
            for parent in parents:
                print(f"  • {parent['parent_name']} (weight: {parent['weight']})")
        
        if not children and not parents:
            print(f"❌ No relationships found for '{skill_name}'")
        
        db.close()
    
    def database_management(self):
        """Database management operations"""
        print("\n🧹 DATABASE MANAGEMENT")
        print("="*30)
        print("1. 🧹 Clear all data")
        print("2. 🔄 Reset frequency data only")
        print("3. 📊 Database statistics")
        print("4. 🔧 Optimize database")
        
        choice = input("\nSelect operation (1-4): ").strip()
        
        try:
            if choice == '1':
                self._clear_all_data()
            elif choice == '2':
                self._reset_frequency_data()
            elif choice == '3':
                self._show_db_statistics()
            elif choice == '4':
                self._optimize_database()
            else:
                print("❌ Invalid choice")
        except Exception as e:
            print(f"❌ Operation failed: {e}")
    
    def _clear_all_data(self):
        """Clear all data from database"""
        confirm = input("⚠️  This will delete ALL data. Type 'CONFIRM' to proceed: ").strip()
        if confirm == 'CONFIRM':
            processor = JobProcessor()
            processor.clear_and_restart()
            print("✅ All data cleared")
            processor.close_connection()
        else:
            print("❌ Operation cancelled")
    
    def _reset_frequency_data(self):
        """Reset only frequency data"""
        confirm = input("Reset frequency data? (y/N): ").lower()
        if confirm == 'y':
            analyzer = SkillFrequencyAnalyzer()
            analyzer.reset_frequencies()
            print("✅ Frequency data reset")
            analyzer.close()
    
    def _show_db_statistics(self):
        """Show detailed database statistics"""
        db = PostgresSkillsPipeline()
        cursor = db.get_cursor()
        
        # Table sizes
        print(f"\n📊 Table Statistics:")
        tables = ['job', 'skill', 'skillgroup', 'skillrelationship', 'frequency', 'competency']
        
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
            count = cursor.fetchone()['count']
            
            # Get table size
            cursor.execute(f"SELECT pg_size_pretty(pg_total_relation_size('{table}')) as size")
            size = cursor.fetchone()['size']
            
            print(f"   {table:15} | {count:6d} rows | {size}")
        
        db.close()
    
    def _optimize_database(self):
        """Optimize database performance"""
        print("🔧 Optimizing database...")
        db = PostgresSkillsPipeline()
        cursor = db.get_cursor()
        
        # Vacuum and analyze
        cursor.execute("VACUUM ANALYZE")
        print("✅ Database optimized")
        
        db.close()
    
    def show_help(self):
        """Show help and system information"""
        print("\n❓ HELP & INFORMATION")
        print("="*30)
        print("""
🎯 SKILLS EXTRACTION & ANALYSIS SYSTEM

This system processes job postings to extract skills, build hierarchies,
and analyze skill frequency across the job market.

📋 WORKFLOW:
1. Job Processing: Extract skills from job JSON files
2. Hierarchy Building: Create parent-child skill relationships  
3. Frequency Analysis: Count skill mentions and propagate up hierarchy
4. Reporting: Generate insights and export data

📁 FILES NEEDED:
- jobs.json: Job postings data (in JSON format)
- skills_hierarchy_definitions.py: Predefined skill relationships

📊 KEY FEATURES:
- Separates job-extracted vs hierarchy-defined skills
- Recursive frequency propagation (parents get children's frequencies)
- Comprehensive reporting and export capabilities
- Interactive query interface

💡 TIPS:
- Run complete pipeline for first-time setup
- Use frequency analysis only for updates
- Check system status before major operations
- Export data regularly for backup

🔧 REQUIREMENTS:
- PostgreSQL database running
- Python dependencies installed
- Valid database credentials in postgres_setup.py
        """)
    
    def run(self):
        """Main system loop"""
        print("🎯 Skills Extraction & Analysis System")
        print("Initializing...")
        
        # Test database connection
        if not self.test_database_connection():
            print("❌ Cannot connect to database. Please check your configuration.")
            return
        
        while True:
            try:
                self.show_main_menu()
                choice = input("\nSelect option (1-9): ").strip()
                
                if choice == '1':
                    jobs_file = input("Enter jobs file path (or press Enter for 'jobs.json'): ").strip()
                    if not jobs_file:
                        jobs_file = "jobs.json"
                    self.complete_pipeline(jobs_file)
                
                elif choice == '2':
                    jobs_file = input("Enter jobs file path (or press Enter for 'jobs.json'): ").strip()
                    if not jobs_file:
                        jobs_file = "jobs.json"
                    self.job_processing_only(jobs_file)
                
                elif choice == '3':
                    self.frequency_analysis_only()
                
                elif choice == '4':
                    self.view_system_status()
                
                elif choice == '5':
                    self.generate_reports()
                
                elif choice == '6':
                    self.interactive_queries()
                
                elif choice == '7':
                    self.database_management()
                
                elif choice == '8':
                    self.show_help()
                
                elif choice == '9':
                    print("\n👋 Thank you for using Skills Analysis System!")
                    break
                
                else:
                    print("❌ Invalid choice. Please select 1-9.")
                
                input("\nPress Enter to continue...")
                
            except KeyboardInterrupt:
                print("\n\n⚠️  Operation interrupted by user.")
                break
            except Exception as e:
                print(f"\n❌ Unexpected error: {e}")
                import traceback
                traceback.print_exc()
                input("\nPress Enter to continue...")


if __name__ == "__main__":
    system = SkillsSystem()
    system.run()

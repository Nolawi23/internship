# Realistic skill hierarchy with adjusted weights
# Weight Philosophy: "How essential is the child skill to effectively using the parent skill?"
# 0.8-0.9: Absolutely essential, can't really use parent without child
# 0.7-0.8: Very important for most use cases
# 0.6-0.7: Important but alternatives exist
# 0.5-0.6: Useful but not critical

skill_hierarchy_with_weights = {
    "Programming Languages": [
        {"child": "Python", "weight": 0.8},          # Very versatile, widely used
        {"child": "JavaScript", "weight": 0.8},      # Essential for web development
        {"child": "Java", "weight": 0.7},            # Important for enterprise
        {"child": "C++", "weight": 0.7},             # Important for performance-critical
        {"child": "R", "weight": 0.6},               # Niche, mainly statistics
        {"child": "Go", "weight": 0.6},              # Good but newer, alternatives exist
        {"child": "C#", "weight": 0.6},              # Mainly Microsoft ecosystem
        {"child": "Swift/Kotlin", "weight": 0.5}     # Very mobile-specific
    ],
    
    "Python": [
        {"child": "NumPy", "weight": 0.7},           # Essential for data science, not general Python
        {"child": "Pandas", "weight": 0.7},          # Same as NumPy
        {"child": "Scikit-learn", "weight": 0.6},    # ML specific, not general Python
        {"child": "TensorFlow", "weight": 0.6},      # Deep learning specific
        {"child": "PyTorch", "weight": 0.6},         # Alternative to TensorFlow
        {"child": "Keras", "weight": 0.5}            # Now integrated into TensorFlow
    ],
    
    "R": [
        {"child": "Statistical Computing", "weight": 0.8}  # This is what R is mainly for
    ],
    
    "Java": [
        {"child": "Backend Development", "weight": 0.8},     # Primary Java use case
        {"child": "Enterprise Applications", "weight": 0.7}  # Important but specific
    ],
    
    "JavaScript": [
        {"child": "React", "weight": 0.7},           # Popular but one of many frameworks
        {"child": "Node.js", "weight": 0.8},         # Essential for backend JS
        {"child": "Angular", "weight": 0.6},         # Alternative to React
        {"child": "Vue.js", "weight": 0.6}           # Alternative to React
    ],
    
    "C#": [
        {"child": ".NET Development", "weight": 0.8},  # Primary C# ecosystem
        {"child": "Game Development", "weight": 0.6}   # Unity specific
    ],
    
    "C++": [
        {"child": "Performance-Critical Applications", "weight": 0.8},  # Main C++ use
        {"child": "Game Development", "weight": 0.7}                    # Important use case
    ],
    
    "Go": [
        {"child": "Backend Development", "weight": 0.7},
        {"child": "Cloud Infrastructure", "weight": 0.7}
    ],
    
    "Swift/Kotlin": [
        {"child": "Mobile Development (iOS/Android)", "weight": 0.9}  # This is what they're for
    ],
    
    "SQL": [
        {"child": "PostgreSQL", "weight": 0.7},      # Popular but one of many
        {"child": "MySQL NoSQL interactions", "weight": 0.7},  # Common combination
        {"child": "Oracle", "weight": 0.6},          # Enterprise but expensive
        {"child": "SQL Server", "weight": 0.6},      # Microsoft ecosystem
        {"child": "GraphQL", "weight": 0.5}          # Different paradigm, not really SQL
    ],
    
    "NoSQL": [
        {"child": "MongoDB", "weight": 0.8},         # Most popular NoSQL
        {"child": "Redis", "weight": 0.7},           # Important for caching
        {"child": "Cassandra", "weight": 0.6}        # Specific use cases
    ],
    
    "Solidity": [
        {"child": "Smart Contracts", "weight": 0.9},      # This is what Solidity is for
        {"child": "Blockchain Development", "weight": 0.8} # Primary use case
    ],
    
    "Web3 Technologies": [
        {"child": "Blockchain", "weight": 0.9},           # Core concept
        {"child": "Cryptography", "weight": 0.8},         # Essential for security
        {"child": "Web3Frameworks", "weight": 0.7},       # Important for development
        {"child": "Decentralized Storage", "weight": 0.6}, # Important but specific
        {"child": "Blockchain Platforms", "weight": 0.7},  # Need to pick platforms
        {"child": "Web3 Infrastructure", "weight": 0.6}    # Helpful but not essential
    ],
    
    "Blockchain": [
        {"child": "Solidity", "weight": 0.8},                          # Most popular smart contract language
        {"child": "Blockchain Architecture", "weight": 0.8},           # Essential concept
        {"child": "Decentralized Applications (dApps)", "weight": 0.7}, # Important application
        {"child": "Vyper", "weight": 0.5}                              # Alternative to Solidity
    ],
    
    "Cryptography": [
        {"child": "Hashing", "weight": 0.9},              # Fundamental
        {"child": "Digital Signatures", "weight": 0.8},   # Very important
        {"child": "Encryption Algorithms", "weight": 0.8}, # Essential
        {"child": "Zero-Knowledge Proofs", "weight": 0.6}  # Advanced topic
    ],
    
    "Web3 Infrastructure": [
        {"child": "IPFS", "weight": 0.7},
        {"child": "Oracles", "weight": 0.7},
        {"child": "The Graph", "weight": 0.6}
    ],
    
    "Decentralized Storage": [
        {"child": "IPFS", "weight": 0.8},        # Most popular
        {"child": "Arweave", "weight": 0.6},     # Alternative
        {"child": "Filecoin", "weight": 0.6}     # Alternative
    ],
    
    "Blockchain Platforms": [
        {"child": "Ethereum", "weight": 0.8},    # Most established
        {"child": "Solana", "weight": 0.6},      # Popular alternative
        {"child": "Polygon", "weight": 0.6},     # L2 solution
        {"child": "Cosmos", "weight": 0.5}       # Interoperability focused
    ],
    
    "Web3Frameworks": [
        {"child": "Web3.js", "weight": 0.8},     # Standard for Ethereum
        {"child": "Ethers.js", "weight": 0.7},   # Modern alternative
        {"child": "Hardhat", "weight": 0.7},     # Development environment
        {"child": "Truffle", "weight": 0.6}      # Older development framework
    ],
    
    "Data Engineering": [
        {"child": "Data Warehousing", "weight": 0.8},      # Core concept
        {"child": "Data Modeling", "weight": 0.8},         # Essential skill
        {"child": "Data Pipelines", "weight": 0.8},        # Core functionality
        {"child": "Data Transformation", "weight": 0.8},   # Essential process
        {"child": "Data Architecture", "weight": 0.7},     # Important for scale
        {"child": "Cloud Computing", "weight": 0.7},       # Modern requirement
        {"child": "Big Data Technologies", "weight": 0.6}, # Not needed for all data engineering
        {"child": "Data Storage", "weight": 0.7},          # Important concept
        {"child": "Stream Processing", "weight": 0.6},     # Specific use cases
        {"child": "Message Queues", "weight": 0.6},        # Specific architecture pattern
        {"child": "Data Governance", "weight": 0.6}        # Important but often separate role
    ],
    
    "Data Warehousing": [
        {"child": "ETL Processes", "weight": 0.9}  # This is what data warehousing is about
    ],
    
    "Big Data Technologies": [
        {"child": "Spark", "weight": 0.8},         # Most versatile
        {"child": "Hadoop", "weight": 0.7},        # Foundation but less direct use
        {"child": "Kafka", "weight": 0.7},         # Important for streaming
        {"child": "Hive", "weight": 0.6}           # SQL on Hadoop
    ],
    
    "Cloud Computing": [
        {"child": "AWS (S3, EC2, EMR, Redshift, Glue)", "weight": 0.8},  # Market leader
        {"child": "Azure (Synapse, Data Factory, Databricks)", "weight": 0.6},  # Alternative
        {"child": "GCP (BigQuery, Dataflow, Dataproc)", "weight": 0.6}          # Alternative
    ],
    
    "Data Storage": [
        {"child": "Relational Databases", "weight": 0.8},  # Still very common
        {"child": "Data Lakes", "weight": 0.7},            # Modern approach
        {"child": "NoSQL Databases", "weight": 0.6}        # Specific use cases
    ],
    
    "Data Governance": [
        {"child": "Data Quality", "weight": 0.8},
        {"child": "Data Lineage", "weight": 0.7},
        {"child": "Metadata Management", "weight": 0.7}
    ],
    
    "Data Pipelines": [
        {"child": "Airflow", "weight": 0.8},       # Most popular orchestrator
        {"child": "Prefect", "weight": 0.6},       # Modern alternative
        {"child": "Cron", "weight": 0.6}           # Basic scheduling
    ],
    
    "Data Transformation": [
        {"child": "Data Cleaning", "weight": 0.9},         # Essential
        {"child": "Data Validation", "weight": 0.8},       # Very important
        {"child": "Feature Engineering", "weight": 0.7},   # Important for ML
        {"child": "Data Normalization", "weight": 0.7},    # Database concept
        {"child": "Data Modeling", "weight": 0.7}          # Important skill
    ],
    
    "Message Queues": [
        {"child": "RabbitMQ", "weight": 0.7},
        {"child": "Amazon SQS", "weight": 0.7},
        {"child": "ActiveMQ", "weight": 0.6}
    ],
    
    "Stream Processing": [
        {"child": "Apache Kafka", "weight": 0.8},   # Most popular
        {"child": "Apache Flink", "weight": 0.6},   # Advanced stream processing
        {"child": "Apache Storm", "weight": 0.5}    # Older technology
    ],
    
    "Machine Learning": [
        {"child": "Machine Learning Algorithms", "weight": 0.9},     # Core of ML
        {"child": "Model Evaluation & Selection", "weight": 0.8},    # Essential skill
        {"child": "Feature Engineering", "weight": 0.8},             # Critical for success
        {"child": "Model Performance Monitoring", "weight": 0.7},    # Important for production
        {"child": "Deep Learning", "weight": 0.6},                   # Subset of ML, not always needed
        {"child": "Model Versioning", "weight": 0.6},                # MLOps concept
        {"child": "MLOps", "weight": 0.6},                          # Production ML
        {"child": "Model Explainability", "weight": 0.6}            # Important but specialized
    ],
    
    "Machine Learning Algorithms": [
        {"child": "Regression", "weight": 0.9},           # Fundamental
        {"child": "Classification", "weight": 0.9},       # Fundamental
        {"child": "Clustering", "weight": 0.7},           # Unsupervised learning
        {"child": "Time Series Analysis", "weight": 0.6}, # Specific domain
        {"child": "Anomaly Detection", "weight": 0.6}     # Specific use case
    ],
    
    "Deep Learning": [
        {"child": "CNNs", "weight": 0.8},          # Image processing
        {"child": "Transformers", "weight": 0.8},  # NLP and modern AI
        {"child": "RNNs", "weight": 0.6},          # Sequence data, less popular now
        {"child": "Autoencoders", "weight": 0.6}   # Specific architecture
    ],
    
    "Model Evaluation & Selection": [
        {"child": "Metrics (Accuracy, Precision, Recall, F1-score, AUC)", "weight": 0.9},  # Essential
        {"child": "Cross-validation", "weight": 0.8},                                       # Important technique
        {"child": "Hyperparameter Tuning", "weight": 0.7}                                  # Performance optimization
    ],
    
    "Model Explainability": [
        {"child": "SHAP values", "weight": 0.8},   # Most popular method
        {"child": "LIME", "weight": 0.6},          # Alternative method
        {"child": "InterpretML", "weight": 0.5}    # Microsoft's approach
    ],
    
    "Feature Engineering": [
        {"child": "Data Preprocessing", "weight": 0.9},    # Essential first step
        {"child": "Feature Selection", "weight": 0.8},     # Important for performance
        {"child": "Feature Transformation", "weight": 0.7} # Scaling, encoding, etc.
    ],
    
    "Model Performance Monitoring": [
        {"child": "Drift Detection (Concept Drift, Data Drift)", "weight": 0.8},
        {"child": "Performance Degradation Monitoring", "weight": 0.7},
        {"child": "Alerting", "weight": 0.7}
    ],
    
    "Model Versioning": [
        {"child": "MLflow", "weight": 0.8},         # Most popular
        {"child": "DVC", "weight": 0.6},            # Data version control
        {"child": "Weights & Biases", "weight": 0.6} # Experiment tracking focus
    ],
    
    "MLOps": [
        {"child": "Model Deployment & Monitoring (MLOps)", "weight": 0.8},  # Core MLOps
        {"child": "Experiment Tracking", "weight": 0.7},                    # Important for organization
        {"child": "Model Serving Infrastructure", "weight": 0.7},           # Production deployment
        {"child": "Feature Stores", "weight": 0.6}                          # Advanced MLOps
    ],
    
    "Experiment Tracking": [
        {"child": "MLflow Tracking", "weight": 0.8},
        {"child": "Weights & Biases", "weight": 0.7}
    ],
    
    "Feature Stores": [
        {"child": "Feast", "weight": 0.7},
        {"child": "Tecton", "weight": 0.6}
    ],
    
    "Model Serving Infrastructure": [
        {"child": "Kubernetes", "weight": 0.7},    # Container orchestration
        {"child": "Istio", "weight": 0.5},         # Service mesh, advanced
        {"child": "Knative", "weight": 0.5}        # Serverless on K8s
    ],
    
    "Model Deployment & Monitoring (MLOps)": [
        {"child": "TensorFlow Serving", "weight": 0.7},  # TensorFlow specific
        {"child": "TorchServe", "weight": 0.6},           # PyTorch specific
        {"child": "SageMaker", "weight": 0.6}             # AWS specific
    ],
    
    "Generative AI": [
        {"child": "Large Language Models (LLMs)", "weight": 0.9},  # Core of current GenAI
        {"child": "Generative Models", "weight": 0.7},             # Broader category
        {"child": "Multimodal AI", "weight": 0.6}                  # Emerging area
    ],
    
    "Large Language Models (LLMs)": [
        {"child": "Prompt Engineering", "weight": 0.8},              # Essential skill
        {"child": "Fine-tuning", "weight": 0.7},                     # Advanced technique
        {"child": "GPT family (GPT-3, GPT-4, etc.)", "weight": 0.8}, # Most popular
        {"child": "BERT", "weight": 0.6}                             # Important but older
    ],
    
    "Generative Models": [
        {"child": "GANs", "weight": 0.7},           # Important generative model
        {"child": "Diffusion Models", "weight": 0.8}, # Currently very popular
        {"child": "VAEs", "weight": 0.6}            # Less common now
    ],
    
    "Multimodal AI": [
        {"child": "Text Processing", "weight": 0.8},
        {"child": "Image Processing", "weight": 0.7},
        {"child": "Speech Processing", "weight": 0.6}
    ],
    
    "Data Analysis": [
        {"child": "Statistical Analysis", "weight": 0.8},      # Core of data analysis
        {"child": "Data Visualization", "weight": 0.8},        # Essential for communication
        {"child": "Data Wrangling/Cleaning", "weight": 0.9}    # Most time-consuming part
    ],
    
    "Data Visualization": [
        {"child": "Matplotlib", "weight": 0.7},    # Python standard
        {"child": "Tableau", "weight": 0.7},       # Business standard
        {"child": "Plotly", "weight": 0.6},        # Interactive plots
        {"child": "Seaborn", "weight": 0.6},       # Statistical visualization
        {"child": "Power BI", "weight": 0.6},      # Microsoft ecosystem
        {"child": "D3.js", "weight": 0.5}          # Web-based, advanced
    ],
    
    "Statistical Analysis": [
        {"child": "Descriptive Statistics", "weight": 0.9},     # Fundamental
        {"child": "Inferential Statistics", "weight": 0.8},     # Important for insights
        {"child": "Probability & Distributions", "weight": 0.7}, # Theoretical foundation
        {"child": "A/B Testing", "weight": 0.7}                 # Practical application
    ],
    
    "Data Wrangling/Cleaning": [
        {"child": "Data Transformation", "weight": 0.8},
        {"child": "Data Imputation", "weight": 0.7}
    ],
    
    "Software Engineering Principles": [
        {"child": "Version Control", "weight": 0.9},              # Absolutely essential
        {"child": "Testing", "weight": 0.8},                      # Critical for quality
        {"child": "Software Architecture", "weight": 0.7},        # Important for scale
        {"child": "Software Development Lifecycle", "weight": 0.7}, # Process knowledge
        {"child": "Modular Coding", "weight": 0.7}                # Good practices
    ],
    
    "Software Development Lifecycle": [
        {"child": "Agile", "weight": 0.8},         # Industry standard
        {"child": "Waterfall", "weight": 0.5}      # Legacy approach
    ],
    
    "Version Control": [
        {"child": "GitHub", "weight": 0.7},        # Platform, not core Git
        {"child": "GitLab", "weight": 0.6}         # Alternative platform
    ],
    
    "Testing": [
        {"child": "Unit Testing", "weight": 0.8},         # Fundamental
        {"child": "Integration Testing", "weight": 0.7},   # Important
        {"child": "End-to-End Testing", "weight": 0.6}    # Higher level
    ],
    
    "Software Architecture": [
        {"child": "Design Patterns (MVC, Microservices)", "weight": 0.8},
        {"child": "System Design (Scalability, Reliability, Performance)", "weight": 0.8},
        {"child": "API Design (RESTful, GraphQL)", "weight": 0.7}
    ],
    
    "Modular Coding": [
        {"child": "SOLID Principles", "weight": 0.8},      # Fundamental principles
        {"child": "Code Reusability", "weight": 0.7},      # Good practice
        {"child": "Design Patterns", "weight": 0.7}        # Reusable solutions
    ],
    
    "Soft Skills": [
        {"child": "Communication", "weight": 0.9},        # Essential in all roles
        {"child": "Problem-Solving", "weight": 0.9},      # Core of technical work
        {"child": "Teamwork", "weight": 0.8},             # Important for collaboration
        {"child": "Adaptability", "weight": 0.7},         # Important in tech
        {"child": "Business Acumen", "weight": 0.6}       # Helpful but not always essential
    ],
    
    "Communication": [
        {"child": "Written Communication", "weight": 0.8},  # Documentation, emails
        {"child": "Presentation Skills", "weight": 0.7}     # Meetings, demos
    ],
    
    "Problem-Solving": [
        {"child": "Analytical Thinking", "weight": 0.9},   # Core skill
        {"child": "Critical Thinking", "weight": 0.8},     # Important for evaluation
        {"child": "Decision-Making", "weight": 0.7}        # Leadership skill
    ],
    
    "Teamwork": [
        {"child": "Collaboration", "weight": 0.8},         # Essential
        {"child": "Active Listening", "weight": 0.7},      # Important for communication
        {"child": "Conflict Resolution", "weight": 0.6}    # Leadership skill
    ],
    
    "Adaptability": [
        {"child": "Learning New Technologies", "weight": 0.8}, # Essential in tech
        {"child": "Time Management", "weight": 0.7},           # Important for productivity
        {"child": "Prioritization", "weight": 0.7}             # Important for focus
    ],
    
    "Business Acumen": [
        {"child": "Understanding Business Goals & Metrics", "weight": 0.8},
        {"child": "Strategic Thinking", "weight": 0.6}
    ],
    
    "DevOps/MLOps": [
        {"child": "Containerization", "weight": 0.8},                                # Modern deployment
        {"child": "Continuous Integration/Continuous Deployment (CI/CD)", "weight": 0.8}, # Essential practice
        {"child": "Monitoring & Logging", "weight": 0.7},                            # Important for production
        {"child": "Infrastructure as Code (IaC)", "weight": 0.6}                     # Advanced practice
    ],
    
    "Continuous Integration/Continuous Deployment (CI/CD)": [
        {"child": "GitHub Actions", "weight": 0.7},    # Popular and integrated
        {"child": "Jenkins", "weight": 0.7},           # Enterprise standard
        {"child": "GitLab CI", "weight": 0.6}          # GitLab integrated
    ],
    
    "Containerization": [
        {"child": "Docker", "weight": 0.9},            # Fundamental
        {"child": "Kubernetes", "weight": 0.7},        # Orchestration
        {"child": "Docker Compose", "weight": 0.6}     # Local development
    ],
    
    "Infrastructure as Code (IaC)": [
        {"child": "Terraform", "weight": 0.8},         # Most popular
        {"child": "CloudFormation", "weight": 0.6},    # AWS specific
        {"child": "Ansible", "weight": 0.6}            # Configuration management
    ],
    
    "Monitoring & Logging": [
        {"child": "Prometheus", "weight": 0.7},        # Popular monitoring
        {"child": "Grafana", "weight": 0.7},           # Visualization
        {"child": "ELK stack", "weight": 0.6}          # Logging stack
    ],
    
    "Cybersecurity": [
        {"child": "Security Best Practices", "weight": 0.8},     # Essential knowledge
        {"child": "Data Security & Privacy", "weight": 0.7},     # Important compliance
        {"child": "Vulnerability Assessment", "weight": 0.6}     # Specialized skill
    ],
    
    "Security Best Practices": [
        {"child": "OWASP Top 10", "weight": 0.8},      # Web security fundamentals
        {"child": "Secure Coding", "weight": 0.8},     # Essential practice
        {"child": "Security Auditing", "weight": 0.6}  # Specialized skill
    ],
    
    "Vulnerability Assessment": [
        {"child": "Penetration Testing", "weight": 0.8},
        {"child": "Security Scanning", "weight": 0.7}
    ],
    
    "Data Security & Privacy": [
        {"child": "Data Encryption", "weight": 0.8},   # Technical implementation
        {"child": "GDPR", "weight": 0.7},              # European regulation
        {"child": "CCPA", "weight": 0.6}               # California regulation
    ],
    
    "Data/Model Version Control": [
        {"child": "Data Version Control", "weight": 0.8},  # Important for reproducibility
        {"child": "Model Version Control", "weight": 0.7}  # MLOps practice
    ],
    
    "Data Version Control": [
        {"child": "DVC", "weight": 0.8},               # Most popular
        {"child": "Delta Lake", "weight": 0.6},        # Databricks ecosystem
        {"child": "LakeFS", "weight": 0.5}             # Git-like for data lakes
    ],
    
    "Model Version Control": [
        {"child": "MLflow Model Registry", "weight": 0.8},
        {"child": "Weights & Biases", "weight": 0.6}
    ],
    
    "Domain-Specific Knowledge": [
        {"child": "Industry-Specific Knowledge", "weight": 0.9}  # Essential for domain expertise
    ],
    
    "Industry-Specific Knowledge": [
        {"child": "Finance", "weight": 0.8},           # Large tech sector
        {"child": "Healthcare", "weight": 0.7},        # Growing tech adoption
        {"child": "Supply Chain", "weight": 0.6}       # Specific domain
    ]
}

# Simple hierarchy for backward compatibility
skill_hierarchy = {}
for parent, children in skill_hierarchy_with_weights.items():
    skill_hierarchy[parent] = [child["child"] for child in children]

# Function to get relationship weight
def get_relationship_weight(parent_skill, child_skill):
    """
    Get weight for a specific parent-child relationship
    
    Args:
        parent_skill (str): Parent skill name
        child_skill (str): Child skill name
        
    Returns:
        float: Weight value, or None if relationship not found
    """
    if parent_skill in skill_hierarchy_with_weights:
        for relationship in skill_hierarchy_with_weights[parent_skill]:
            if relationship["child"] == child_skill:
                return relationship["weight"]
    return None

# Function to get all relationships sorted by weight
def get_relationships_by_weight(min_weight=0.0):
    """
    Get all parent-child relationships with weight above threshold
    
    Args:
        min_weight (float): Minimum weight threshold
        
    Returns:
        list: List of relationships sorted by weight (highest first)
    """
    all_relationships = []
    
    for parent, children in skill_hierarchy_with_weights.items():
        for relationship in children:
            if relationship["weight"] >= min_weight:
                all_relationships.append({
                    "parent": parent,
                    "child": relationship["child"],
                    "weight": relationship["weight"]
                })
    
    return sorted(all_relationships, key=lambda x: x["weight"], reverse=True)

# Weight analysis function
def analyze_skill_importance():
    """Analyze which skills have highest weights across relationships"""
    skill_weights = {}
    
    for parent, children in skill_hierarchy_with_weights.items():
        for relationship in children:
            child = relationship["child"]
            weight = relationship["weight"]
            
            if child not in skill_weights:
                skill_weights[child] = []
            skill_weights[child].append(weight)
    
    # Calculate average weight per skill
    avg_weights = {}
    for skill, weights in skill_weights.items():
        avg_weights[skill] = sum(weights) / len(weights)
    
    return sorted(avg_weights.items(), key=lambda x: x[1], reverse=True)

# Weight distribution analysis
def get_weight_distribution():
    """Get distribution of weights across all relationships"""
    all_weights = []
    for parent, children in skill_hierarchy_with_weights.items():
        for relationship in children:
            all_weights.append(relationship["weight"])
    
    distribution = {
        "critical_0.8+": len([w for w in all_weights if w >= 0.8]),
        "important_0.7-0.79": len([w for w in all_weights if 0.7 <= w < 0.8]),
        "useful_0.6-0.69": len([w for w in all_weights if 0.6 <= w < 0.7]),
        "optional_0.5-0.59": len([w for w in all_weights if 0.5 <= w < 0.6]),
        "total": len(all_weights),
        "average": sum(all_weights) / len(all_weights)
    }
    
    return distribution

# Example usage and validation
if __name__ == "__main__":
    print("🎯 Realistic Skill Hierarchy with Adjusted Weights Loaded!")
    print(f"📊 Total parent skills: {len(skill_hierarchy_with_weights)}")
    
    total_relationships = sum(len(children) for children in skill_hierarchy_with_weights.values())
    print(f"📊 Total relationships: {total_relationships}")
    
    # Show weight distribution
    dist = get_weight_distribution()
    print(f"\n📈 Weight Distribution:")
    print(f"   🔴 Critical (0.8+): {dist['critical_0.8+']} relationships")
    print(f"   🟠 Important (0.7-0.79): {dist['important_0.7-0.79']} relationships") 
    print(f"   🟡 Useful (0.6-0.69): {dist['useful_0.6-0.69']} relationships")
    print(f"   🟢 Optional (0.5-0.59): {dist['optional_0.5-0.59']} relationships")
    print(f"   📊 Average weight: {dist['average']:.2f}")
    
    # Show top 10 highest weighted relationships
    top_relationships = get_relationships_by_weight()[:10]
    print(f"\n🏆 Top 10 Most Critical Relationships:")
    for i, rel in enumerate(top_relationships, 1):
        print(f"{i:2d}. {rel['parent']} → {rel['child']} [{rel['weight']}]")
    
    # Show some examples of realistic adjustments
    print(f"\n💡 Examples of Realistic Weight Adjustments:")
    examples = [
        ("Version Control", "GitHub", "0.7 (platform, not core Git)"),
        ("Machine Learning", "Deep Learning", "0.6 (subset, not always needed)"),
        ("Python", "TensorFlow", "0.6 (ML-specific, not general Python)"),
        ("SQL", "PostgreSQL", "0.7 (popular but one of many)"),
        ("Data Wrangling/Cleaning", "Data Transformation", "0.8 (essential process)")
    ]
    
    for parent, child, explanation in examples:
        weight = get_relationship_weight(parent, child)
        print(f"   • {parent} → {child}: {weight} - {explanation}")

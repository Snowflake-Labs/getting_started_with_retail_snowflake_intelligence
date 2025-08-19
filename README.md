[![Snowflake - Certified](https://img.shields.io/badge/Snowflake-Certified-2ea44f?style=for-the-badge&logo=snowflake)](https://developers.snowflake.com/solutions/)
# Retail Intelligence

This comprehensive demo showcases **Retail Intelligence** capabilities for AI-powered retail analytics with customer sentiment analysis using Lather and Leaf hand soap company as a real-world example. You'll explore Semantic Models, Snowflake Intelligence, Cortex Analyst to build a complete end-to-end solution. Follow the step-by-step setup to experience how Snowflake's unified platform enables intelligent, data-driven decision making.

## 🚀 Quick Start

Get up and running with this demo in just a few steps:

1. **Setup**: Run the setup script to create all necessary Snowflake objects
2. **Experience**: Follow the Snowflake Intelligence demo prompts to explore AI-powered analytics

For detailed instructions, see the [Getting Started](#-getting-started) section below.

## 📚 What You'll Learn

🗄️ **Database Architecture** - Design and implement scalable database structures
🔧 **Object Management** - Create and manage databases, schemas, and data objects
📋 **Semantic Modeling** - Build consistent data definitions and metadata layers
🤖 **AI & ML** - Leverage Snowflake Cortex for intelligent data processing
💭 **NLP Techniques** - Implement sentiment analysis and text processing pipelines
🏗️ **Modern Architecture** - Implement cloud-native data engineering patterns

## 🛠️ What You'll Build

🏛️ **Production Database** - Complete setup with all necessary objects and permissions
🎛️ **Interactive Dashboard** - Real-time web application for data visualization
🔗 **Semantic Layer** - Consistent metadata and business logic definitions
🧠 **ML Pipeline** - End-to-end machine learning workflow with model deployment

## 📋 Prerequisites

☁️ **Snowflake Account** - Active account with appropriate privileges
📝 **SQL Knowledge** - Basic understanding of SQL syntax and concepts
🤖 **Cortex Access** - Snowflake account with Cortex features enabled
📊 **ML Concepts** - Understanding of machine learning fundamentals
📚 **Data Analytics** - Basic understanding of data analysis principles

## Repository Structure
```
├── README.md                 # This file
├── LEGAL.md                 # Legal notice
├── LICENSE                  # License information
├── notebooks/               # Snowflake Notebooks
│   ├── environment.yml     # Package dependencies
│   └── 0_start_here.ipynb  # Main notebook to start with
├── scripts/                # SQL and configuration files
│   ├── setup.sql          # Database setup script
│   ├── teardown.sql       # Cleanup script
│   └── semantic_models/   # Semantic models and configurations
└── streamlit/             # Streamlit applications
    ├── app.py             # Main Streamlit app
    └── requirements.txt   # Python dependencies
```

## 🏁 Getting Started

Follow these steps to get the demo running:

### 1️⃣ Database Setup

Run the setup script to create all necessary Snowflake objects:

```sql
-- Execute in Snowflake worksheet
-- Copy and paste contents of scripts/setup.sql
```

This will create:
- Database: `SNOWFLAKE_INTELLIGENCE`
- Warehouse: `RETAIL_SNOWFLAKE_INTELLIGENCE_WH`
- Role: `RETAIL_SNOWFLAKE_INTELLIGENCE_ROLE`
- Tables and views for data analysis

### 2️⃣ Interactive Analysis

Open and run the Snowflake notebook:

1. Navigate to Snowflake → **Projects** → **Notebooks**
2. Import `notebooks/0_start_here.ipynb`
3. Follow the step-by-step analysis workflow

### 3️⃣ Interactive Dashboard

Launch the Streamlit application:

```bash
# Install dependencies
pip install -r streamlit/requirements.txt

# Run the app
streamlit run streamlit/app.py
```

The dashboard will be available at `http://localhost:8501`

### 4️⃣ Cleanup (Optional)

When you're done exploring, clean up the resources:

```sql
-- Execute in Snowflake worksheet
-- Copy and paste contents of scripts/teardown.sql
```

## 🔧 Configuration

The setup script will create all necessary Snowflake objects including:
- 🗄️ Databases: SNOWFLAKE_INTELLIGENCE, RETAIL_SNOWFLAKE_INTELLIGENCE_DB
- 📂 Schemas: AGENTS, ANALYTICS
- ⚡ Warehouses: RETAIL_SNOWFLAKE_INTELLIGENCE_WH
- 👤 Roles: RETAIL_SNOWFLAKE_INTELLIGENCE_ROLE
- 📋 Tables & Views: 9 data objects for analysis

## 📊 Data & Analytics

This demo showcases advanced analytics capabilities including:

- 💭 **Sentiment Analysis** - Process and analyze text data to understand customer opinions
- 📊 **Statistical Modeling** - Apply statistical methods to derive business insights  
- 🔍 **Data Exploration** - Interactive tools for discovering patterns and trends
- 📈 **Visualization** - Rich charts and dashboards for data storytelling

## 🎯 Key Features

✨ **Production Ready** - Enterprise-grade setup with proper permissions and security
🚀 **Fast Setup** - Get running in minutes with automated database creation
📱 **Interactive UI** - User-friendly Streamlit dashboard for data exploration
📓 **Guided Learning** - Step-by-step notebooks with detailed explanations
🤖 **AI-Powered** - Leverages Snowflake Cortex for intelligent data processing
🔗 **Semantic Layer** - Consistent data definitions and business logic
💭 **NLP Capabilities** - Advanced text analysis and sentiment processing
🧹 **Easy Cleanup** - Automated teardown script for resource management
📚 **Well Documented** - Comprehensive guides and inline documentation

## 🤝 Contributing

Found an issue or want to contribute? We welcome contributions! Please:
1. Fork this repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🆘 Support

- 💬 [Snowflake Community](https://community.snowflake.com/)
- 📖 [Snowflake Documentation](https://docs.snowflake.com/)
- 🚀 [QuickStart Guide](https://quickstarts.snowflake.com/guide/teardown-fix-test)

---

*This demo showcases the power of Snowflake's Data Cloud for modern analytics and AI workloads.*

[![Snowflake - Certified](https://img.shields.io/badge/Snowflake-Certified-2ea44f?style=for-the-badge&logo=snowflake)](https://developers.snowflake.com/solutions/)

# Retail Snowflake Intelligence

this repo contains everything you need to get started with Snowflake Intelligence with retail data. You will learn how Snowflake Intelligence leverages natural language to drive deep insights from you data. This example uses a fictional hand soap company, "Lather and Leaf," to demonstrate a common retail use case. For a full tutorial, view the quickstart [here](https://quickstarts.snowflake.com/guide/retail_snowflake_intelligence).

## Repository Structure
```
├── README.md                 # This file
├── LEGAL.md                 # Legal notice
├── LICENSE                  # License information
├── scripts/                # SQL and configuration files
│   ├── setup.sql          # Database setup script
│   ├── teardown.sql       # Cleanup script
│   └── semantic_models/   # Semantic models and configurations
```

## 📋 Getting Started

***

### **Required Setup**

Building a Cortex Agent for Snowflake Intelligence involves three main steps: creating the necessary Snowflake objects, uploading your semantic model files, and finally, creating and configuring the agent itself in Snowsight.

-----
### **1. Setting Up the Environment**

First, you'll need to create the database, schema, and a named internal stage where your semantic model files will be stored. This ensures a clean and organized environment for your demo.

Run the SQL commands in scripts/setup.sql in a Snowflake worksheet

-----

### **2. Uploading Semantic Model Files via Snowsight**

Once the stage is created, you can manually upload your semantic model YAML files directly through the Snowsight user interface.

1.  **Navigate to your Stage in Snowsight:**

      * In the Snowsight left-hand navigation pane, go to **Data**.
      * Expand your database (`RETAIL_SNOWFLAKE_INTELLIGENCE_DB`).
      * Expand the schema (`ANALYTICS`).
      * Click on **Stages**.
      * Click on the `SEMANTIC_MODELS` stage.

2.  **Upload the Files:**

      * Download the files from scripts/semantic_models/
      * Click the **+ Upload** button in the top-right corner.
      * A file explorer window will open.
      * Select your three semantic model YAML files (`product_comments_and_stats.yaml`, `product_inventory.yaml`, `product_sales_analysis.yaml`) from your local machine.
      * Click **Open** to begin the upload. You will see a progress bar for each file, and they will appear in the stage's file list once complete.

-----

### **3. Creating the Cortex Agent**

With your files staged, you can now create the agent in the Snowflake UI and connect it to your semantic models.

1.  **Navigate to the Snowflake Intelligence UI:**

      * In Snowsight, go to **AI & ML**.
      * Select **Agents**.
      * Click the **+ Create Agent** button.

2.  **Fill in the Agent Details:**

      * Give your agent a **name** (e.g., `Retail_Analytics_Agent`).
      * Provide a **description** that explains the agent's purpose, such as "An agent to help a direct-to-consumer company analyze sales, inventory, and customer sentiment."

3.  **Add the Semantic Models as Tools:**

      * In the agent creation wizard, go to the **Tools** section.
      * Under the "Cortex Analyst" heading, click the **+ Add** button.
      * Select **Semantic Model File**.
      * Choose the database, schema, and stage you created (`RETAIL_SNOWFLAKE_INTELLIGENCE_DB`, `ANALYTICS`, `SEMANTIC_MODELS`).
      * Select each of your three YAML files from the list (`sales_model.yaml`, `inventory_model.yaml`, `comments_model.yaml`).
      * Select `RETAIL_SNOWFLAKE_INTELLIGENCE_WH` warehouse
      * Add a timeout of 60 seconds
      * Have Cortex create a description 
      * Click **Add**. This will add all three models to your agent.

4.  **Finalize and Create:**

      * Review the agent's details and tools.
      * Click **Create Agent**.

Your new Cortex Agent is now configured and ready to be used in the Snowflake Intelligence chat. You can now use the prompts from the demo to test its functionality.

***

### **Prompts for Snowflake Intelligence**

These are the key prompts that can be used to explore Snowflake Intelligence, designed to be copy-pasted directly into the Snowflake Intelligence chat. They demonstrate a progression from simple to complex, multi-source questions.

1.  **How are the sales for my 2 newest scents doing?**
    * **Purpose:** This initial query shows Snowflake Intelligence’s ability to summarize structured sales data.

2.  **Are our 2 new scents getting a lot of positive buzz online?**
    * **Purpose:** This highlights the platform’s capability to analyze unstructured data (social media comments, reviews) and provide a sentiment overview.

3.  **What are the latest comments for Peachwood Breeze?**
    * **Purpose:** This illustrates how the platform can retrieve specific, unstructured data points (text comments) to provide tangible evidence that supports the analysis.

4.  **If Peachwood Breeze sells online like previous top performing scents, what will be the likely sales online over the next 12 weeks? And do I have enough inventory to fulfill those sales from my online distribution center?**
    * **Purpose:** This powerful prompt showcases **predictive analytics** and **multi-source querying**, combining a sales forecast with an inventory check.

5.  **I want to be safe and handle online sales of up to 16,000 units. If that happens, are there any distribution centers that are likely to have extra inventory so we can transfer to meet online demand?**
    * **Purpose:** The final prompt shows the platform's ability to handle **"what-if" scenarios** and provide a concrete, actionable business recommendation.

***

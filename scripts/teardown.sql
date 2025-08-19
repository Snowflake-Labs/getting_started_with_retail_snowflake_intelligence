/*--
 Retail Intelligence - Teardown Script
 This script removes all objects created by the setup script
--*/

USE ROLE accountadmin;

-- Drop database (cascades to all schemas, tables, views, etc.)
DROP DATABASE IF EXISTS SNOWFLAKE_INTELLIGENCE;

-- Drop database (cascades to all schemas, tables, views, etc.)
DROP DATABASE IF EXISTS RETAIL_SNOWFLAKE_INTELLIGENCE_DB;

-- Drop warehouse
DROP WAREHOUSE IF EXISTS RETAIL_SNOWFLAKE_INTELLIGENCE_WH;

-- Drop custom role
DROP ROLE IF EXISTS RETAIL_SNOWFLAKE_INTELLIGENCE_ROLE;

SELECT 'Teardown completed successfully! All demo objects have been removed.' as status;

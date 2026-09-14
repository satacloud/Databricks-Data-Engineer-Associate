# Databricks notebook source
# MAGIC %sql
# MAGIC -- Create safe function for catalog name
# MAGIC CREATE OR REPLACE TEMPORARY FUNCTION safe_uc_name(value STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN
# MAGIC   COALESCE(
# MAGIC     NULLIF(
# MAGIC       REGEXP_REPLACE(
# MAGIC         REGEXP_REPLACE(
# MAGIC           LOWER(TRIM(value)),
# MAGIC           '[^a-z0-9_]',
# MAGIC           '_'
# MAGIC         ),
# MAGIC         '_+',
# MAGIC         '_'
# MAGIC       ),
# MAGIC       ''
# MAGIC     ),
# MAGIC     'user'
# MAGIC   );
# MAGIC
# MAGIC
# MAGIC -- Create temp view of all available catalogs
# MAGIC CREATE OR REPLACE TEMPORARY VIEW list_of_catalogs AS
# MAGIC SELECT LOWER(catalog_name) AS catalog_names
# MAGIC FROM system.information_schema.catalogs;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Start SQL scripting block to create and or set catalog based on Workspace
# MAGIC BEGIN
# MAGIC
# MAGIC -- =========================================
# MAGIC -- 1. Declare variables
# MAGIC -- =========================================
# MAGIC
# MAGIC   -- Leave catalog_forced as NONE to use the default labuser_yourusername catalog. 
# MAGIC   -- IF you already have a catalog you want to use instead, replace NONE with your catalog name.
# MAGIC   DECLARE catalog_forced STRING DEFAULT 'NONE';
# MAGIC
# MAGIC
# MAGIC   DECLARE user_email STRING;
# MAGIC   DECLARE user_name STRING;
# MAGIC   -- Make the user name safe as a catalog name
# MAGIC   DECLARE safe_user_name STRING;
# MAGIC   -- Set's the vocareum catalog name 'labuser1234123423'
# MAGIC   DECLARE vocareum_catalog_name STRING DEFAULT 'NA';
# MAGIC   -- Catalog name to use outside of Vocareum
# MAGIC   DECLARE catalog_name STRING DEFAULT 'NA';
# MAGIC   -- Flag to determine if the catalog already exists
# MAGIC   DECLARE catalog_already_exists BOOLEAN DEFAULT FALSE;
# MAGIC   -- Create catalog statement to run
# MAGIC   DECLARE create_catalog_statement STRING DEFAULT '';
# MAGIC   -- View to create the catalog name as view to use outside of this script
# MAGIC   DECLARE create_view_with_catalog_name STRING DEFAULT '';
# MAGIC
# MAGIC
# MAGIC -- =========================================
# MAGIC -- 2. Capture current user
# MAGIC -- =========================================
# MAGIC   SET user_email = current_user();
# MAGIC   SET user_name = SPLIT(user_email, '@')[0];
# MAGIC   SET safe_user_name = (SELECT safe_uc_name(SPLIT(current_user(), '@')[0]));
# MAGIC
# MAGIC -- ==================================================================================
# MAGIC -- 3. Determine Vocareum or Non Vocareum Workspace and set and/or create catalog (Non Vocareum)
# MAGIC -- ==================================================================================
# MAGIC   -- Check to see if the user is in Vocareum by email address then setting the catalog name to the labuser name.
# MAGIC   IF (LOWER(user_email) LIKE '%@vocareum.com')
# MAGIC     THEN 
# MAGIC       SET vocareum_catalog_name = safe_user_name;
# MAGIC       
# MAGIC       -- Check existence using the temp view of catalogs
# MAGIC       IF EXISTS (
# MAGIC         SELECT 1
# MAGIC         FROM list_of_catalogs
# MAGIC         WHERE catalog_names = LOWER(vocareum_catalog_name)
# MAGIC       ) THEN  
# MAGIC
# MAGIC         -- Document the catalog already exists
# MAGIC         SET catalog_already_exists = TRUE;
# MAGIC
# MAGIC         -- Create Temp View to Store the SQL Variable Value for Global Use
# MAGIC         SET create_view_with_catalog_name = CONCAT(
# MAGIC               "CREATE OR REPLACE TEMP VIEW catalog_name_vw AS ",
# MAGIC               "SELECT '", vocareum_catalog_name, "' AS catalog_name"
# MAGIC             );
# MAGIC
# MAGIC
# MAGIC       END IF; 
# MAGIC
# MAGIC   -- If Vocareum check fails, setup assuming you are in another Workspace like Free Edition. Create a catalog using labuser_username
# MAGIC   ELSE
# MAGIC
# MAGIC     -- Checks to see if a catalog is forced to be used. Will use that but the catalog has to exist already. 
# MAGIC     IF catalog_forced != 'NONE' THEN
# MAGIC
# MAGIC       -- Use the forced catalog by the user
# MAGIC       SET catalog_name = catalog_forced;
# MAGIC
# MAGIC       -- Tests to confirm the catalog exists. Error is returned if it doesn't.
# MAGIC       USE CATALOG IDENTIFIER(catalog_name);
# MAGIC
# MAGIC       -- Document the catalog already exists
# MAGIC       SET catalog_already_exists = TRUE;
# MAGIC
# MAGIC       -- Create Temp View to Store the SQL Variable Value for Global Use
# MAGIC       SET create_view_with_catalog_name = CONCAT(
# MAGIC         "CREATE OR REPLACE TEMP VIEW catalog_name_vw AS ",
# MAGIC         "SELECT '", catalog_name, "' AS catalog_name"
# MAGIC       );
# MAGIC
# MAGIC     -- If catalog is not forced, create default catalog for learner
# MAGIC     ELSE
# MAGIC
# MAGIC       -- Limit the user's name to 19 characters. THis is done because there is a limit to the catalog.schema.object name (64 characters). For someone with a long name this could cause issus. Using 19 because that is the general size of the vocareum user name
# MAGIC       SET catalog_name = CONCAT('labuser_', LEFT(safe_user_name,19));
# MAGIC
# MAGIC       -- Create Temp View to Store the SQL Variable Value for Global Use
# MAGIC       SET create_view_with_catalog_name = CONCAT(
# MAGIC         "CREATE OR REPLACE TEMP VIEW catalog_name_vw AS ",
# MAGIC         "SELECT '", catalog_name, "' AS catalog_name"
# MAGIC       );
# MAGIC
# MAGIC       -- Check existence using the temp view of catalogs
# MAGIC       IF EXISTS (
# MAGIC         SELECT 1
# MAGIC         FROM list_of_catalogs
# MAGIC         WHERE catalog_names = LOWER(catalog_name)
# MAGIC       ) 
# MAGIC         THEN 
# MAGIC         
# MAGIC           -- Document if the catalog already exists
# MAGIC           SET catalog_already_exists = TRUE;
# MAGIC       
# MAGIC       ELSE 
# MAGIC         -- Otherwise, create the catalog for the user
# MAGIC         SET create_catalog_statement = CONCAT(
# MAGIC           'CREATE CATALOG IF NOT EXISTS ',
# MAGIC           catalog_name
# MAGIC         );
# MAGIC
# MAGIC         -- Create the catalog
# MAGIC         EXECUTE IMMEDIATE create_catalog_statement;
# MAGIC         
# MAGIC         -- Document if the catalog didn't exist
# MAGIC         SET catalog_already_exists = FALSE;
# MAGIC
# MAGIC       END IF;
# MAGIC
# MAGIC     END IF;
# MAGIC
# MAGIC   END IF;
# MAGIC
# MAGIC   -- Create the temp view with the catalog name
# MAGIC   EXECUTE IMMEDIATE create_view_with_catalog_name;
# MAGIC
# MAGIC
# MAGIC -- ===================================================================
# MAGIC -- 4. Return Final Script Results (Workspace Assumed and Catalog Name
# MAGIC -- ===================================================================
# MAGIC   SELECT
# MAGIC     CASE
# MAGIC       WHEN vocareum_catalog_name <> 'NA' THEN 'Vocareum Workspace'
# MAGIC       ELSE 'Non Vocareum Workspace'
# MAGIC     END AS Workspace,
# MAGIC     CASE
# MAGIC       WHEN vocareum_catalog_name <> 'NA' THEN vocareum_catalog_name
# MAGIC       ELSE catalog_name
# MAGIC     END AS `User Catalog Name`,
# MAGIC     CASE
# MAGIC       WHEN catalog_already_exists = TRUE THEN 'Catalog Already Exists'
# MAGIC       ELSE 'Catalog Created'
# MAGIC     END AS `Catalog Status`;
# MAGIC END;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the global SQL variable with the catalog name using the temp view
# MAGIC DECLARE OR REPLACE my_catalog STRING;
# MAGIC SET VAR my_catalog = (SELECT catalog_name FROM catalog_name_vw);
# MAGIC
# MAGIC
# MAGIC -- Set default catalog to user's labuser catalog
# MAGIC USE CATALOG IDENTIFIER(my_catalog);
# MAGIC
# MAGIC
# MAGIC -- Create lab schema for learner
# MAGIC
# MAGIC DECLARE OR REPLACE my_schema STRING;
# MAGIC SET VAR my_schema = 'data_ingestion';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS IDENTIFIER(my_schema);
# MAGIC USE SCHEMA IDENTIFIER(my_schema);

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VIEW IF EXISTS catalog_name_vw;
# MAGIC DROP VIEW IF EXISTS list_of_catalogs;
# MAGIC DROP TABLE IF EXISTS sql_csv_autoloader;
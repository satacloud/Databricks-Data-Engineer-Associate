# Databricks notebook source
## Determines if in Vocareum or Other Workspace and sets up the catalog
## Usage: my_catalog = build_user_catalog() within your demo/lab setup.

import re
from typing import Optional

def _safe_uc_name(value: str) -> str:
    # UC identifiers are generally safest with letters, numbers, underscores
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9_]", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value or "user"


def _current_user_email() -> str:
    """
    Get the user's name and email address.
    """
    return spark.sql("SELECT current_user()").first()[0]


def _get_workspace_catalogs():
    """
    Returns a set of Catalogs visible to that user.
    """
    list_of_catalogs_in_workspace = [row["catalog"].strip().lower() for row in spark.sql("SHOW CATALOGS").collect()]
    return list_of_catalogs_in_workspace


def _catalog_exists(name: str, catalogs: set[str]) -> bool:
    """
    Catalog checker to see if the catalog already exists for that user.
    """
    catalog_exists = name.lower() in catalogs
    return catalog_exists


def build_user_catalog(prefix: str = "labuser", catalog_forced = None) -> str:
    """
    Returns a UC catalog name for the current user.

    Parameters
    ----------
    prefix: str
        Prefix for the catalog name. Default is 'labuser'.
    catalog_forced: str
        Uses this catalog name if specified. Otherwise uses the prefix and user's name.

    Vocareum behavior:
      - If a catalog equals the user's 'labuserxxx' name and already exists,
        assume you are in Vocareum and use it.
      - Assumes users have a catalog by default in Vocareum.

    Other workspaces:
      - Use <prefix>_<user> and create it if possible for that user.

    Example:
        my_catalog = build_user_catalog(catalog_forced=None)  ## <-- Force the usage of a catalog if you can't create one.
    """

    # Obtain user's email and user name name
    user_email = _current_user_email()
    user_name = user_email.split("@")[0]

    # Make the user name safe if it's not in Vocareum
    safe_user_name = _safe_uc_name(user_name)


    # VOCAREUM CHECKER: Catalog is just the username (already provisioned)
    # and starts with 'labuser'
    vocareum_catalog_name = safe_user_name

    if user_email.lower().endswith("@vocareum.com"):
        print("✅ Vocareum workspace detected.")

        if _catalog_exists(
            name=vocareum_catalog_name,
            catalogs=_get_workspace_catalogs()
        ):
            print(f"✅ Using existing Vocareum catalog: '{vocareum_catalog_name}'.")
            return vocareum_catalog_name
        else:
            raise ValueError(
                f"❌ Catalog '{vocareum_catalog_name}' does not exist in this Vocareum workspace. "
                "Please create the catalog or verify the catalog name before continuing."
            )

    # OTHER WORKSPACE SETUP
    else:
        print("ℹ️ Non-Vocareum workspace detected. Setting up catalog.")

        # Setting catalog for workspaces outside of Vocareum using the provided prefix and user name
        # Limit the user's name to 19 characters. THis is done because there is a limit to the catalog.schema.object name (64 characters). For someone with a long name this could cause issus. Using 19 because that is the general size of the vocareum user name
        safe_user_name_char_restrict = safe_user_name[:19]

        # If catalog_forced is set, will use that by default.
        if catalog_forced is None:
            catalog_name = f"{prefix}_{safe_user_name_char_restrict}"
            print(f"ℹ️ Using default catalog name: '{catalog_name}'.")
        else:
            catalog_name = catalog_forced
            print(f"ℹ️ Using specified catalog name: '{catalog_name}'.")


        # Check if the user already has this catalog with the prefix_safeusername
        if _catalog_exists(name=catalog_name, catalogs=_get_workspace_catalogs()) == True:
            print(f"✅ Catalog '{catalog_name}' already exists. Using this catalog.")
            return catalog_name
        elif _catalog_exists(name=catalog_name, catalogs=_get_workspace_catalogs()) == False and catalog_forced is not None:
            raise RuntimeError(
                f"❌ Catalog '{catalog_name}' does not exist in this workspace. "
                "A forced catalog name must reference an existing catalog. "
                "Create the catalog or reference an existing one, then rerun the notebook."
            )
        else:
            try:
                print(f"ℹ️ Catalog '{catalog_name}' not found. Creating it now...")
                spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
                print(f"✅ Catalog '{catalog_name}' created successfully.")
                return catalog_name
            except Exception as e:
                print(
                    f"⚠️ Could not create catalog '{catalog_name}'. "
                    "You may not have privileges to create catalogs in this workspace.\n"
                    f"Error: {e}"
                )

# COMMAND ----------

def create_volume(in_catalog: str, in_schema: str, volume_name: str):
    '''
    Create a volume in the specified catalog.schema.
    '''
    print(f'Creating volume: {in_catalog}.{in_schema}.{volume_name} if not exists.\n')
    r = spark.sql(f'CREATE VOLUME IF NOT EXISTS {in_catalog}.{in_schema}.{volume_name}')

# COMMAND ----------

def delete_source_files(source_files: str):
    """
    Deletes all files in the specified source volume.

    This function iterates through all the files in the given volume,
    deletes them, and prints the name of each file being deleted.

    Parameters:
    - source_files : str
        The path to the volume containing the files to delete. 
        Use the {DA.paths.working_dir} to dynamically navigate to the user's volume location in dbacademy/ops/vocareumlab@name:
            Example: DA.paths.working_dir = /Volumes/dbacademy/ops/vocareumlab@name

    Returns:
    - None. This function does not return any value. It performs file deletion and prints all files that it deletes. If no files are found it prints in the output.

    Example:
    - delete_source_files(f'{DA.paths.working_dir}/pii/stream_source/user_reg')
    """

    import os

    print(f'\nSearching for files in {source_files} volume to delete prior to creating files...')
    if os.path.exists(source_files):
        list_of_files = sorted(os.listdir(source_files))
    else:
        list_of_files = None

    if not list_of_files:  # Checks if the list is empty.
        print(f"No files found in {source_files}.\n")
    else:
        for file in list_of_files:
            file_to_delete = source_files + file
            print(f'Deleting file: {file_to_delete}')
            dbutils.fs.rm(file_to_delete)

# COMMAND ----------

import os

def create_directory_in_user_volume(user_default_volume_path: str, create_folders: list):
    '''
    Creates multiple (or single) directories in the specified volume path.

    Args:
    -------
        user_default_volume_path (str): The base directory path where the folders will be created. 
                                        You can use the default DA.paths.working_dir as the user's volume path.
        create_folders (list): A list of strings representing folder names to be created within the base directory.

    Returns:
    -------
        None: This function does not return any values but prints log information about the created directories.

    Example: 
    -------
    create_directory_in_user_volume(user_default_volume_path=DA.paths.working_dir, create_folders=['customers', 'orders', 'status'])
    '''
    
    print('----------------------------------------------------------------------------------------')
    for folder in create_folders:

        create_folder = f'{user_default_volume_path}/{folder}'

        if not os.path.exists(create_folder):
        # If it doesn't exist, create the directory
            dbutils.fs.mkdirs(create_folder)
            print(f'Creating folder: {create_folder}')

        else:
            print(f"Directory {create_folder} already exists. No action taken.")
        
    print('----------------------------------------------------------------------------------------\n')

# COMMAND ----------

def copy_files(copy_from: str, copy_to: str, n: int, sleep=2):
    '''
    Copy files from one location to another destination's volume.

    This method performs the following tasks:
      1. Lists files in the source directory and sorts them. Sorted to keep them in the same order when copying for consistency.
      2. Verifies that the source directory has at least `n` files.
      3. Copies files from the source to the destination, skipping files already present at the destination.
      4. Pauses for `sleep` seconds after copying each file.
      5. Stops after copying `n` files or if all files are processed.
      6. Will print information on the files copied.
    
    Parameters
    - copy_from (str): The source directory where files are to be copied from.
    - copy_to (str): The destination directory where files will be copied to.
    - n (int): The number of files to copy from the source. If n is larger than total files, an error is returned.
    - sleep (int, optional): The number of seconds to pause after copying each file. Default is 2 seconds.

    Returns:
    - None: Prints information to the log on what files it's loading. If the file exists, it skips that file.

    Example:
    - copy_files(copy_from='/Volumes/gym_data/v01/user-reg', 
           copy_to=f'{DA.paths.working_dir}/pii/stream_source/user_reg',
           n=1)
    '''
    import os
    import time

    print(f"\n----------------Loading files to user's volume: '{copy_to}'----------------")

    ## List all files in the copy_from volume and sort the list
    list_of_files_to_copy = sorted(os.listdir(copy_from))
    total_files_in_copy_location = len(list_of_files_to_copy)

    ## Get a list of files in the source
    list_of_files_in_source = os.listdir(copy_to)

    assert total_files_in_copy_location >= n, f"The source location contains only {total_files_in_copy_location} files, but you specified {n}  files to copy. Please specify a number less than or equal to the total number of files available."

    ## Looping counter
    counter = 1

    ## Load files if not found in the co
    for file in list_of_files_to_copy:
      if file.startswith('_'):
        pass
      else:
        ## If the file is found in the source, skip it with a note. Otherwise, copy file.
        if file in list_of_files_in_source:
          print(f'File number {counter} - {file} is already in the source volume "{copy_to}". Skipping file.')
        else:
          file_to_copy = f'{copy_from}{file}'
          copy_file_to = f'{copy_to}{file}'
          print(f'File number {counter} - Copying file {file_to_copy} --> {copy_file_to}.')
          dbutils.fs.cp(file_to_copy, copy_file_to , recurse = True)
          
          ## Sleep after load
          time.sleep(sleep) 

        ## Stop after n number of loops based on argument.
        if counter == n:
          break
        else:
          counter = counter + 1

# COMMAND ----------

def drop_tables(in_catalog: str, in_schema: list, dry_run: bool = False):
    """
    Drops all tables and views in the specified schema within a given catalog.

    Args:
        in_catalog (str): The catalog name (e.g., 'dbacademy_peter').
        in_schema (str): The schema name (e.g., 'default').
        dry_run (bool): If True, only prints tables that would be dropped without actually dropping them.

    Returns:
        list: Fully qualified names of the tables that were dropped (or would be dropped in dry-run mode).

    Example:
    >>> drop_tables(in_catalog='dbacademy_peter', in_schema='1_bronze_db')
    """
    # Check if catalog exists
    catalogs = [row.catalog for row in spark.sql("SHOW CATALOGS").collect()]
    if in_catalog not in catalogs:
        raise ValueError(f"Catalog '{in_catalog}' does not exist.")

    ## Delete tables and views in schema
    for schema in in_schema:
        
        # Check if schema exists in the catalog
        full_schema = f"{in_catalog}.{schema}"
        if not spark.catalog.databaseExists(full_schema):
            raise ValueError(f"Schema '{schema}' does not exist in catalog '{in_catalog}'.")
        
        print(f"\n{'Previewing' if dry_run else 'Dropping'} all tables in {in_catalog}.{schema}:")

        # Get all tables in the schema
        tables = spark.sql(f"SHOW TABLES IN {in_catalog}.{schema}").collect()

        if not tables:
            print(f"No tables found in schema {in_catalog}.{schema}. Nothing to drop.")
        else:
            table_names = [f"{in_catalog}.{schema}.{t.tableName}" for t in tables]

            for table_full_name in table_names:
                if dry_run:
                    print(f"Would drop: {table_full_name}")
                else:
                    try:
                        spark.sql(f"DROP TABLE IF EXISTS {table_full_name}")
                        print(f"Dropped TABLE: {table_full_name}")
                    except:
                        spark.sql(f"DROP VIEW IF EXISTS {table_full_name}")
                        print(f"Dropped VIEW: {table_full_name}")

# COMMAND ----------

def display_config_values(config_values, copy_values=False):
    """
    Displays list of key-value pairs as rows of HTML text.

    Parameters
    ----------
    config_values : list of (key, value[, copy_value]) tuples
        key: text to display in "information" column
        value: text to display in "value" column (HTML safe; can include links)
        copy_value: per-row override for copy button. Can be specified as True or as a string value
                    if you want to copy something different than what's displayed. For example,
                    value might display a folder name while copy_value includes the entire path
    copy_values : bool, optional
        If True, a copy button appears next to each value. Default is False.

    Returns
    ----------
    HTML output displaying the config values

    Example
    --------
    display_config_values([('catalog', 'your catalog'), ('schema', 'your schema')])
    display_config_values([('catalog', 'your catalog')], copy_values=True)
    """
    rows = ""
    for name, value, *rest in config_values:
        copy_value = rest[0] if rest else (copy_values or "")
        copy_btn = ""
        if copy_value:
            if isinstance(copy_value, str):
                copy_value = f'data-copy="{copy_value}"'
            else:
                copy_value = ""
                
            copy_btn = """
                <button type="button" onclick="copyFromSibling(this)"
                    style="margin-left:auto;padding:2px 10px;border:1px solid #ccc;border-radius:4px;background:#f5f5f5;cursor:pointer;font-size:13px;flex:0 0 auto;min-width:68px; text-align:center; box-sizing:border-box;">
                    Copy
                </button>"""

        rows += f"""
        <tr>
          <td style="padding:6px 12px;white-space:nowrap;border-bottom:1px solid #e0e0e0;font-weight:600">{name}:</td>
          <td style="padding:6px 12px;border-bottom:1px solid #e0e0e0">
            <div style="display:flex; align-items:center; gap:12px;">
                <span class="copy-source" {copy_value} style="display:inline-block;padding:4px 8px;font-size:15px;min-width:0;">{value}</span>
                {copy_btn}
            </div>
          </td>
        </tr>"""

    html = """
    <div style="font-family:system-ui,-apple-system,sans-serif;max-width:1100px;margin:12px 0;border:1px solid #e0e0e0;border-radius:8px;overflow:hidden">
      <div style="background:#1b3a4b;color:#fff;padding:10px 16px;font-size:16px;font-weight:600">Configuration Values</div>
      <table style="width:100%;border-collapse:collapse;font-size:15px">
        <tr style="background:#f5f5f5">
          <th style="padding:6px 12px;text-align:left;border-bottom:1px solid #e0e0e0">Information</th>
          <th style="padding:6px 12px;text-align:left;border-bottom:1px solid #e0e0e0">Value</th>
        </tr>""" + rows + """
      </table>
    </div>
    <script>
        if (!window.copyFromSibling) {
            window.copyFromSibling = function(btn) {
            var source = btn.parentElement.querySelector('.copy-source');
            if (!source) return;

            var text = source.dataset.copy || source.innerText;

            var t = document.createElement('textarea');
            t.value = text;
            t.style.position = 'fixed';
            t.style.opacity = '0';
            t.style.pointerEvents = 'none';
            document.body.appendChild(t);
            t.select();
            document.execCommand('copy');
            document.body.removeChild(t);

            var original = btn.textContent;
            btn.textContent = 'Copied!';
            setTimeout(function() {
                btn.textContent = original;
            }, 1500);
            };
        }
    </script>"""
    displayHTML(html)

# COMMAND ----------

my_catalog = build_user_catalog(catalog_forced= None)
spark.sql(f'USE CATALOG {my_catalog}')

spark.sql(f'DECLARE OR REPLACE VARIABLE my_catalog STRING')
spark.sql(f"SET VAR my_catalog = '{my_catalog}'")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS data_ingestion;
# MAGIC USE SCHEMA data_ingestion;

# COMMAND ----------

create_volume(in_catalog = my_catalog,
              in_schema = 'data_ingestion',
              volume_name = 'landing_folder')

user_vol_path = f'/Volumes/{my_catalog}/data_ingestion/landing_folder'

# COMMAND ----------

create_directory_in_user_volume(user_default_volume_path = user_vol_path, 
                                create_folders = ['csv_demo_files', 'json_demo_files', 'xml_demo_files'])
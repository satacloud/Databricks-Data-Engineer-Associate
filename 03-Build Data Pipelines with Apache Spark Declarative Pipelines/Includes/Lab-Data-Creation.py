# Databricks notebook source
import os  
import pandas as pd
from io import StringIO

class LabDataSetup:
    """
    Sets up lab data by checking for the existence of a catalog, schema, volume and creating the CSV files in a staging volume.

    - Catalog, schema and volume must already exist.

    Example:
      obj = LabDataSetup('dbacademy_peter_s','default','lab_staging_files')
    """

    def __init__(self, catalog_name: str, schema_name: str, volume_name: str):
        self.catalog_name = catalog_name
        self.schema_name = schema_name
        self.volume_name = volume_name
        self.volume_path = os.path.join('/Volumes', self.catalog_name, self.schema_name, self.volume_name)

        print("Starting environment validation...")
        self._validate_environment()

        dict_of_files = {
            'employees_1.csv': self.create_csv_1_data(),
            'employees_2.csv': self.create_csv_2_data(),
            'employees_3.csv': self.create_csv_3_data()
        }

        for filename, filefunc in dict_of_files.items():
            self.create_csv_file_if_not_exists(file_name=filename, csv_data_func=filefunc)

        print(f"LabDataSetup initialized successfully in volume_path: '{self.volume_path}'")

    def _validate_environment(self):
        self._validate_catalog()
        self._validate_schema()
        self._validate_volume()

    def _validate_catalog(self):
        catalogs = spark.sql("SHOW CATALOGS").collect()
        catalog_names = [row.catalog for row in catalogs]
        if self.catalog_name in catalog_names:
            print(f"Catalog '{self.catalog_name}' exists.")
        else:
            print(f"Catalog '{self.catalog_name}' does not exist.")
            raise FileNotFoundError(f"{self.catalog_name} catalog not found.")

    def _validate_schema(self):
        full_schema_name = f"{self.catalog_name}.{self.schema_name}"
        if spark.catalog.databaseExists(full_schema_name):
            print(f"Schema '{full_schema_name}' exists.")
        else:
            print(f"Schema '{full_schema_name}' does not exist.")
            raise FileNotFoundError(f"{full_schema_name} schema not found.")

    def _validate_volume(self):
        volumes = spark.sql(f"SHOW VOLUMES IN {self.catalog_name}.{self.schema_name}").collect()
        volume_names = [v.volume_name for v in volumes]
        if self.volume_name in volume_names:
            print(f"Volume '{self.volume_name}' exists.")
        else:
            print(f"Volume '{self.volume_name}' does not exist.")
            raise FileNotFoundError(f"{self.volume_name} volume not found.")

    def check_if_file_exists(self, file_name: str) -> bool:
        file_path = os.path.join(self.volume_path, file_name)
        if os.path.exists(file_path):
            print(f"The file '{file_path}' exists.")
            return True
        else:
            print(f"The file '{file_path}' does not exist.")
            return False

    def create_csv_file(self, csv_string: str, file_to_create: str):
        df = pd.read_csv(StringIO(csv_string))
        output_path = os.path.join(self.volume_path, file_to_create)
        df.to_csv(output_path, index=False)
        print(f"Created CSV file at '{output_path}'.")

    def create_csv_file_if_not_exists(self, file_name: str, csv_data_func: callable):
        if not self.check_if_file_exists(file_name=file_name):
            print(f"Creating file '{file_name}'...")
            self.create_csv_file(csv_string=csv_data_func, file_to_create=file_name)
        else:
            print(f"File '{file_name}' already exists. Skipping creation.")

    def create_csv_1_data(self) -> str:
        return """EmployeeID,FirstName,Country,Department,Salary,HireDate,Operation,ProcessDate
null,test,test,test,9999,2025-01-01,new,2025-06-05
1,Sophia,US,Sales,72000,2025-04-01,new,2025-06-05
2,Nikos,Gr,IT,55000,2025-04-10,new,2025-06-05
3,Liam,US,Sales,69000,2025-05-03,new,2025-06-05
4,Elena,GR,IT,53000,2025-06-04,new,2025-06-05
5,James,Us,IT,60000,2025-06-05,new,2025-06-05"""

    def create_csv_2_data(self) -> str:
        return """EmployeeID,FirstName,Country,Department,Salary,HireDate,Operation,ProcessDate
6,Emily,us,Enablement,80000,2025-06-09,new,2025-06-22
7,Yannis,gR,HR,70000,2025-06-20,new,2025-06-22
3,Liam,US,Sales,100000,2025-05-03,update,2025-06-22
1,,,,,,delete,2025-06-22"""

    def create_csv_3_data(self):
        return """EmployeeID,FirstName,Country,Department,Salary,HireDate,Operation,ProcessDate
8,Panagiotis,Gr,Enablement,90000,2025-07-01,new,2025-07-22
6,,,,,,delete,2025-07-22
2,,,,,,delete,2025-07-22"""

    def copy_file(self, copy_file: str, to_target_volume: str):
        dbutils.fs.cp(f'{self.volume_path}/{copy_file}', f'{to_target_volume}/{copy_file}')
        print(f"Moving file '{self.volume_path}/{copy_file}' to '{to_target_volume}/{copy_file}'.")

    def delete_lab_staging_files(self):
        dbutils.fs.rm(self.volume_path, True)
        print(f"Deleted all files in '{self.volume_path}'.")

    def __str__(self):
        return f"LabDataSetup(catalog_name={self.catalog_name}, schema_name={self.schema_name}, volume_name={self.volume_name}, volume_path={self.volume_path})"
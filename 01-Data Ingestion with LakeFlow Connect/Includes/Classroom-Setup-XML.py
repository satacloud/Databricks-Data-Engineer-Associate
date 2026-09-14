# Databricks notebook source
# MAGIC %run ./Classroom-Setup-Common

# COMMAND ----------

# Create a sample xml file in xml volume

xmlString = """
  <books>
    <book id="222">
      <author>Corets, Eva</author>
      <title>Maeve Ascendant</title>
    </book>
    <book id="333">
      <author>Corets, Eva</author>
      <title>Oberon's Legacy</title>
    </book>
  </books>"""

xmlPath = f"/Volumes/{my_catalog}/data_ingestion/landing_folder/xml_demo_files/example_1_data.xml"

r = dbutils.fs.put(xmlPath, xmlString, True)
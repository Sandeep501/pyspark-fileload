# Databricks notebook source
import urllib
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql import functions as F

# COMMAND ----------

def mount_s3(bucketName: str, mountName: str) -> None:
    #For dev it is ok to expose creds but use keyVault preferbally
    AccesskeyID = "AKIA2KGEVPQ3DPIADP3E"
    Secretaccesskey = "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"
    encoded_secret_key = urllib.parse.quote(string = Secretaccesskey, safe="")
    if not any(
        mountName.mountPoint == mountName for mountName in dbutils.fs.mounts()
    ):
        dbutils.fs.mount(f"s3a://{AccesskeyID}:{encoded_secret_key}@{bucketName}", mountName)
        print(f"{mountName} is created")
    else:
        print("mount already exists")

# COMMAND ----------

files_path = "/mnt/awsStore/LOAD_5FILES"

def retrive_file_names(files_path):
    files = list()
    for file in dbutils.fs.ls(files_path):
        if ((file.name).endswith(".csv")) and (file.name) not in ["EMP_01_20230704.csv", "EMP_04_20230702.csv"]:
            files.append(file.path)
    return files
# retrive_file_names(files_path)

# COMMAND ----------

def union_files(files):
    # Initialize an empty DataFrame
    combined_df = None
    # Iterate through the CSV files

    for file in files:
        # Load the CSV file
        rdd = spark.sparkContext.textFile(file)
        
        # Skip the first 12 lines
        rdd_skipped = rdd.zipWithIndex().filter(lambda x: x[1] >= 11).map(lambda x: x[0])

        df = spark.read.csv(rdd_skipped, header = "True")
        
        # Extract the column names of the current CSV file
        current_columns = df.columns
        print(file, current_columns)
 
        
        # Check if it's the first file or if the column names match the existing DataFrame
        if combined_df is None or (set(combined_df.columns) == set(current_columns)):
            # Union the DataFrame if it's the first file or if the column names match
            if combined_df is None:
                combined_df = df
            else:
                combined_df = combined_df.union(df)
        else:
            print(f"Columns in file '{file}' do not match the existing DataFrame.")
            continue
    return combined_df

# COMMAND ----------

# mount S3
bucketName = "mybucketaws0001"
mountName = "/mnt/awsStore"
try:
    mount_s3(bucketName, mountName)
except Exception:
    print(Exception)

s3_files = retrive_file_names(files_path)
df = union_files(s3_files)
display(df)

# COMMAND ----------

dbutils.fs.unmount("/mnt/awsStore")

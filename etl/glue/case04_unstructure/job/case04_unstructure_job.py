import sys
import pandas as pd
import boto3
from io import BytesIO
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Configuration
BUCKET = "gsb-ai-data-driven-data-nonprod-201064226169"
SOURCE_KEY = "preparing/case04_unstructure/case04_unstructure_20260408.xlsx"
OUTPUT_PATH = "s3://gsb-ai-data-driven-data-nonprod-201064226169/staging/case04_unstructure/load/"
SHEETS = ["Personal", "MicroFinance", "NonRetailLoan"]
SKIP_ROWS = 2  # Skip header rows (row 1 = column names, row 2 = sub-headers)

# Column mapping: Excel columns -> target column names
COLUMNS = [
    "LN_TYPE", "GL_ACCOUNT_ID", "CID", "COST_CENTER",
    "AMOUNT", "RATE_INT", "PROVCAT", "STAGE",
    "COVERAGE_RATIO", "EAD", "ECL", "PRV_ECL", "UNDRAWN"
]

# Read Excel from S3
s3 = boto3.client("s3")
response = s3.get_object(Bucket=BUCKET, Key=SOURCE_KEY)
excel_bytes = BytesIO(response["Body"].read())

# Read each sheet and combine
all_dfs = []
for sheet_name in SHEETS:
    df = pd.read_excel(
        excel_bytes,
        sheet_name=sheet_name,
        header=None,
        skiprows=SKIP_ROWS,
        dtype=str,  # Read all as string to prevent float conversion
        engine="openpyxl"
    )
    # Only take the first 13 columns (matching our target)
    df = df.iloc[:, :len(COLUMNS)]
    df.columns = COLUMNS
    df["SHEET_NAME"] = sheet_name
    # Drop rows where all values are NaN
    df = df.dropna(how="all", subset=COLUMNS)
    all_dfs.append(df)
    excel_bytes.seek(0)  # Reset for next sheet

# Combine all sheets
combined = pd.concat(all_dfs, ignore_index=True)

# Add VERSION_DATE and ETL_UPDATE_DATE
combined["VERSION_DATE"] = "2026-04-08"
combined["ETL_UPDATE_DATE"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")

# Reorder columns to match target table
final_columns = [
    "VERSION_DATE", "ETL_UPDATE_DATE", "LN_TYPE", "GL_ACCOUNT_ID",
    "CID", "COST_CENTER", "AMOUNT", "RATE_INT", "PROVCAT", "STAGE",
    "COVERAGE_RATIO", "EAD", "ECL", "PRV_ECL", "UNDRAWN", "SHEET_NAME"
]
combined = combined[final_columns]

# Ensure all columns are string type (prevent type merge conflicts between sheets)
combined = combined.fillna("")
combined = combined.astype(str)

# Convert to Spark DataFrame and write as gzip CSV
from pyspark.sql.types import StructType, StructField, StringType
schema = StructType([StructField(col, StringType(), True) for col in combined.columns])
spark_df = spark.createDataFrame(combined, schema=schema)
spark_df.coalesce(1).write.option("header", "true").option("compression", "gzip").mode("overwrite").csv(OUTPUT_PATH)

job.commit()

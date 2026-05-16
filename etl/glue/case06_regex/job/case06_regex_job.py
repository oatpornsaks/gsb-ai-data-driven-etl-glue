import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import regexp_extract, col, when, lit, current_timestamp, to_date, trim, length

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Read log file as single column
df = spark.read.text("s3://gsb-ai-data-driven-data-nonprod-201064226169/preparing/case06_regex/")

# Filter: only lines with CID or CIF queries
df = df.filter(
    (length(trim(col("value"))) > 0) &
    (
        col("value").like("%SELECT CID, ODACN FROM DEPODP WHERE CID IN%") |
        col("value").like("%WHERE CIF.ACN =%")
    )
)

# Extract fields using regex
result = df.select(
    regexp_extract(col("value"), r"([0-3][0-9]\.[0-9][0-9]\.[0-9]{4})", 1).alias("TRANS_DATE_STR"),
    regexp_extract(col("value"), r"([0-2][0-9]:[0-5][0-9]:[0-5][0-9])", 1).alias("TRANS_TIME"),
    regexp_extract(col("value"), r"\[([0-9.]+)\]", 1).alias("IP_ADDRESS"),
    regexp_extract(col("value"), r"\] (/[^ ]+)", 1).alias("URL"),
    regexp_extract(col("value"), r"([0-9]+) ms", 1).alias("RESPONSE_TIME"),
    regexp_extract(col("value"), r"P: ([^ ]+)", 1).alias("USER_ID"),
    regexp_extract(col("value"), r"CID IN \(([^)]+)\)", 1).alias("CID"),
    regexp_extract(col("value"), r"ACN = ([0-9]+)", 1).alias("CIF"),
    when(col("value").like("%ERROR%") | col("value").like("%java%"), lit(1)).otherwise(lit(0)).alias("ERROR_FLAG"),
    col("value").alias("LOG_TXT"),
)

# Add VERSION_DATE and ETL_UPDATE_DATE
result = result.withColumn("TRANS_DATE", to_date(col("TRANS_DATE_STR"), "dd.MM.yyyy")) \
    .drop("TRANS_DATE_STR") \
    .withColumn("VERSION_DATE", to_date(lit("20260408"), "yyyyMMdd")) \
    .withColumn("ETL_UPDATE_DATE", current_timestamp())

# Reorder columns to match target
result = result.select(
    "TRANS_DATE", "TRANS_TIME", "IP_ADDRESS", "URL", "RESPONSE_TIME",
    "USER_ID", "CID", "CIF", "ERROR_FLAG", "LOG_TXT",
    "VERSION_DATE", "ETL_UPDATE_DATE"
)

# Write to staging
result.write.option("header", "true").option("compression", "gzip").mode("overwrite").csv(
    "s3://gsb-ai-data-driven-data-nonprod-201064226169/staging/case06_regex/load/"
)

job.commit()

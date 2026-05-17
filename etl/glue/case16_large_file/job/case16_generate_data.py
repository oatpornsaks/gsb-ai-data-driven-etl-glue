import sys
import uuid
import random
from datetime import date, timedelta
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import Row
from pyspark.sql.types import *

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Configuration: ~100M rows × ~500 bytes = ~50GB
NUM_ROWS = 100000000  # 100 million
NUM_PARTITIONS = 100  # 100 files × ~500MB each

# Reference data
CURRENCIES = ["THB", "USD", "EUR", "JPY", "GBP", "CNY", "SGD", "HKD"]
TX_TYPES = ["TRANSFER", "PAYMENT", "WITHDRAWAL", "DEPOSIT", "PURCHASE", "REFUND", "FEE", "INTEREST"]
MERCHANTS = ["7-Eleven", "Lotus", "Big C", "Central", "SCB", "KBank", "BBL", "TMB", "GSB ATM", "Online Shop", "Grab", "Lazada", "Shopee", "LINE Pay", "TrueMoney"]
CATEGORIES = ["RETAIL", "FOOD", "TRANSPORT", "UTILITY", "FINANCE", "ECOMMERCE", "INSURANCE", "HEALTHCARE"]
BRANCHES = [f"BR{str(i).zfill(4)}" for i in range(1, 201)]
CHANNELS = ["ATM", "MOBILE", "INTERNET", "BRANCH", "POS", "API"]
STATUSES = ["SUCCESS", "PENDING", "FAILED", "REVERSED"]

def generate_partition(partition_id):
    """Generate rows for one partition."""
    rng = random.Random(partition_id)
    rows = []
    rows_per_partition = NUM_ROWS // NUM_PARTITIONS
    base_date = date(2026, 1, 1)
    
    for i in range(rows_per_partition):
        tx_date = base_date + timedelta(days=rng.randint(0, 120))
        rows.append(Row(
            TRANSACTION_ID=str(uuid.UUID(int=rng.getrandbits(128))),
            ACCOUNT_NUMBER=f"{rng.randint(1000000000, 9999999999):010d}",
            TRANSACTION_DATE=tx_date.isoformat(),
            TRANSACTION_TIME=f"{rng.randint(0,23):02d}:{rng.randint(0,59):02d}:{rng.randint(0,59):02d}",
            AMOUNT=f"{rng.uniform(1, 999999):.2f}",
            CURRENCY=rng.choice(CURRENCIES),
            TRANSACTION_TYPE=rng.choice(TX_TYPES),
            MERCHANT_NAME=rng.choice(MERCHANTS),
            MERCHANT_CATEGORY=rng.choice(CATEGORIES),
            BRANCH_CODE=rng.choice(BRANCHES),
            CHANNEL=rng.choice(CHANNELS),
            STATUS=rng.choice(STATUSES),
            REFERENCE_NUMBER=f"REF{rng.randint(100000000000, 999999999999)}",
            VERSION_DATE="2026-04-08",
            ETL_UPDATE_DATE="2026-04-08 09:00:00"
        ))
    return rows

# Generate using Spark parallelism
partition_ids = list(range(NUM_PARTITIONS))
rdd = sc.parallelize(partition_ids, NUM_PARTITIONS).flatMap(generate_partition)

schema = StructType([
    StructField("TRANSACTION_ID", StringType()),
    StructField("ACCOUNT_NUMBER", StringType()),
    StructField("TRANSACTION_DATE", StringType()),
    StructField("TRANSACTION_TIME", StringType()),
    StructField("AMOUNT", StringType()),
    StructField("CURRENCY", StringType()),
    StructField("TRANSACTION_TYPE", StringType()),
    StructField("MERCHANT_NAME", StringType()),
    StructField("MERCHANT_CATEGORY", StringType()),
    StructField("BRANCH_CODE", StringType()),
    StructField("CHANNEL", StringType()),
    StructField("STATUS", StringType()),
    StructField("REFERENCE_NUMBER", StringType()),
    StructField("VERSION_DATE", StringType()),
    StructField("ETL_UPDATE_DATE", StringType()),
])

df = spark.createDataFrame(rdd, schema)
df.write.option("header", "true").option("delimiter", "|").mode("overwrite").csv(
    "s3://gsb-ai-data-driven-data-nonprod-201064226169/raw/case16_large_file/"
)

job.commit()

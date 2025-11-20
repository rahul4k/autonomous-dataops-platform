# Sample fixed PySpark job
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

def run_job():
    spark = SparkSession.builder.appName("SkewJoinJob").getOrCreate()
    
    # Load data
    transactions = spark.read.parquet("s3://bucket/transactions")
    users = spark.read.parquet("s3://bucket/users")
    
    # Fix: Broadcast the smaller table to avoid shuffle and skew
    joined_df = transactions.join(broadcast(users), on="user_id")
    
    # Aggregation
    result = joined_df.groupBy("country").count()
    
    result.write.mode("overwrite").parquet("s3://bucket/output")

if __name__ == "__main__":
    run_job()

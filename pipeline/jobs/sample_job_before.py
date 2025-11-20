# Sample buggy PySpark job
from pyspark.sql import SparkSession

def run_job():
    spark = SparkSession.builder.appName("SkewJoinJob").getOrCreate()
    
    # Load data
    transactions = spark.read.parquet("s3://bucket/transactions")
    users = spark.read.parquet("s3://bucket/users")
    
    # Intentional skew issue: joining on a hot key without handling
    # 'users' is small enough to broadcast but we are doing a standard sort-merge join
    joined_df = transactions.join(users, on="user_id")
    
    # Aggregation that will suffer from skew
    result = joined_df.groupBy("country").count()
    
    result.write.mode("overwrite").parquet("s3://bucket/output")

if __name__ == "__main__":
    run_job()

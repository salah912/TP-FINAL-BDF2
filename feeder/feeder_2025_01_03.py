from pyspark.sql import SparkSession
from feeder.utils.logger import get_logger

def get_spark_session(app_name: str) -> SparkSession:
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .getOrCreate()

def main():
    logger = get_logger("Feeder_0103", "feeder/logs/feeder_2025_01_03.log")
    logger.info("Starting Feeder 2025-01-03...")

    spark = get_spark_session("Feeder 2025-01-03")

    df = spark.read.option("header", True).option("inferSchema", True) \
        .csv("hdfs://namenode:9000/source/raw_us_accidents.csv")
    
    logger.info(f"Total rows in dataset: {df.count()}")

    for col in df.columns:
        if any(c in col for c in [' ', '(', ')', ';', '{', '}', '=', '\n', '\t']):
            df = df.withColumnRenamed(col, col.replace(' ', '_').replace('(', '').replace(')', '').replace('/', '_'))

    _, _, part3 = df.randomSplit([0.33, 0.33, 0.34], seed=42)

    part3 = part3.repartition(3)

    output_path = "hdfs://namenode:9000/bronze/2025/01/03/"
    part3.write.mode("overwrite").parquet(output_path)
    logger.info(f"Part 3 written to {output_path}")

    spark.stop()
    logger.info("Feeder 2025-01-03 finished successfully.")

if __name__ == "__main__":
    main()

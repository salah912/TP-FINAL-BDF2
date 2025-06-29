from pyspark.sql import SparkSession
from utils.logger import get_logger

def get_spark_session(app_name: str) -> SparkSession:
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .getOrCreate()

def main():
    logger = get_logger("Feeder_0101", "feeder/logs/feeder_2025_01_01.log")
    logger.info("Starting Feeder 2025-01-01...")

    spark = get_spark_session("Feeder 2025-01-01")

    # Lecture du fichier source depuis HDFS
    df = spark.read.option("header", True).option("inferSchema", True) \
        .csv("hdfs://namenode:9000/source/raw_us_accidents.csv")
    
    logger.info(f"Total rows in dataset: {df.count()}")

    # Nettoyage des noms de colonnes (caractères interdits)
    for col in df.columns:
        if any(c in col for c in [' ', '(', ')', ';', '{', '}', '=', '\n', '\t']):
            df = df.withColumnRenamed(col, col.replace(' ', '_')
                                                .replace('(', '')
                                                .replace(')', '')
                                                .replace('/', '_'))

    # Découpage en 3 parts
    part1, _, _ = df.randomSplit([0.33, 0.33, 0.34], seed=42)

    # Repartitionnement en 3 fichiers (environ 333 Mo par fichier)
    part1 = part1.repartition(3)

    # Écriture de la première partition
    output_path = "hdfs://namenode:9000/bronze/2025/01/01/"
    part1.write.mode("overwrite").parquet(output_path)
    logger.info(f"Part 1 written to {output_path}")

    spark.stop()
    logger.info("Feeder 2025-01-01 finished successfully.")

if __name__ == "__main__":
    main()

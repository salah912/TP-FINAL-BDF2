from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, dayofmonth, trim, lower, to_timestamp
)
from preprocessor.utils.logger import get_logger

# Initialisation du logger
logger = get_logger("Cleaner", "/tmp/cleaner.log")

# Création de la session Spark avec support Hive
spark = SparkSession.builder \
    .appName("Cleaner") \
    .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Lecture des données depuis la couche Bronze
input_path = "hdfs://namenode:9000/bronze/2025/01/03/*"

try:
    df = spark.read.parquet(input_path)
    logger.info(f"✅ Lecture réussie depuis {input_path}")
except Exception as e:
    logger.error(f"❌ Erreur de lecture depuis {input_path} : {e}")
    spark.stop()
    raise

# Colonnes nécessaires
cols_to_keep = [
    "Start_Time", "Start_Lat", "Start_Lng", "Severity",
    "Description", "City", "State"
]

df_filtered = df.select(*cols_to_keep)

# Nettoyage des données
df_cleaned = df_filtered \
    .withColumn("Start_Time", to_timestamp("Start_Time")) \
    .withColumn("Start_Lat", col("Start_Lat").cast("double")) \
    .withColumn("Start_Lng", col("Start_Lng").cast("double")) \
    .withColumn("Severity", col("Severity").cast("int")) \
    .withColumn("Description", lower(trim(col("Description")))) \
    .withColumn("City", lower(trim(col("City")))) \
    .withColumn("State", lower(trim(col("State")))) \
    .withColumn("year", year("Start_Time")) \
    .withColumn("month", month("Start_Time")) \
    .withColumn("day", dayofmonth("Start_Time"))

# Sauvegarde vers une zone temporaire pour ingestion Hive
output_path = "hdfs://namenode:9000/silver_tmp/"

try:
    df_cleaned.write \
        .mode("overwrite") \
        .partitionBy("year", "month", "day") \
        .parquet(output_path)
    logger.info(f"✅ Données nettoyées écrites dans {output_path}")
except Exception as e:
    logger.error(f"❌ Erreur d'écriture vers {output_path} : {e}")
    spark.stop()
    raise

spark.stop()

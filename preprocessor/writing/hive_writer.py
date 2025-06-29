# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
import logging
import os

# Logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Cleaner")

# Initialisation Spark avec Hive
spark = SparkSession.builder \
    .appName("HiveWriter") \
    .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

logger.info("Session Spark initialisée avec support Hive.")

try:
    df = spark.read.option("basePath", "hdfs://namenode:9000/silver_tmp/") \
        .parquet("hdfs://namenode:9000/silver_tmp/")

    logger.info("Nombre initial de partitions : {}".format(df.rdd.getNumPartitions()))
    df = df.repartition(32)
    logger.info("Nombre de partitions après repartition : {}".format(df.rdd.getNumPartitions()))

    df.createOrReplaceTempView("silver_us_accidents_temp")

    spark.sql("CREATE DATABASE IF NOT EXISTS us_accidents_db")
    spark.sql("USE us_accidents_db")

    spark.sql("DROP TABLE IF EXISTS silver_us_accidents")

    logger.info("Création de la table Hive silver_us_accidents...")
    df.write.mode("overwrite").saveAsTable("us_accidents_db.silver_us_accidents")
    logger.info("✅ Données écrites dans Hive : silver_us_accidents")

except Exception as e:
    logger.error("Erreur globale dans le script : {}".format(str(e)))
finally:
    spark.stop()

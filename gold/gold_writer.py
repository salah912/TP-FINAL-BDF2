# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc

import sys
import os

# Ajout du chemin pour logger.py
sys.path.insert(0, "/tmp/gold/utils")
from logger import get_logger

# Création du logger
log_file = "/tmp/gold_writer.log"
logger = get_logger("GoldWriter", log_file)

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Gold Writer") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    logger.info("Lecture des données Silver...")
    df = spark.read.option("basePath", "hdfs://namenode:9000/silver_tmp") \
               .parquet("hdfs://namenode:9000/silver_tmp/*")

    logger.info("Réduction du nombre de partitions pour améliorer les performances...")
    df = df.repartition(8)

    logger.info("Agrégation : top 20 des villes avec le plus d'accidents")
    top_cities = df.groupBy("City").count().orderBy(desc("count")).limit(20)

    logger.info("Écriture du résultat dans la couche Gold...")
    top_cities.write.mode("overwrite").parquet("/tmp/datamart/top_20_cities")

    logger.info("✅ Données Gold écrites avec succès dans /gold/top_20_cities")

    spark.stop()

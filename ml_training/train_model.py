import sys
import os

sys.path.insert(0, "/app")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

from preprocessor.utils.logger import get_logger

logger = get_logger("MLTraining", "/tmp/ml_training.log")

spark = SparkSession.builder \
    .appName("Train RF Model") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

try:
    logger.info("Reading Gold - top 20 cities")
    top_cities = spark.read.parquet("hdfs://namenode:9000/gold/top_20_cities").select("City")

    logger.info("Reading Silver data")
    df_silver = spark.read.option("basePath", "hdfs://namenode:9000/silver_tmp") \
                          .parquet("hdfs://namenode:9000/silver_tmp")

    logger.info("Joining Silver with top 20 cities")
    df = df_silver.join(top_cities, on="City", how="inner")

    df = df.select("Start_Lat", "Start_Lng", "City", "State", "year", "month", "day", "Severity").dropna()

    logger.info("Repartitioning to 12 max")
    df = df.repartition(12)

    logger.info("Indexing categorical variables")
    indexers = [
        StringIndexer(inputCol="City", outputCol="City_index", handleInvalid="keep"),
        StringIndexer(inputCol="State", outputCol="State_index", handleInvalid="keep")
    ]

    assembler = VectorAssembler(
        inputCols=["Start_Lat", "Start_Lng", "City_index", "State_index", "year", "month", "day"],
        outputCol="features"
    )

    rf = RandomForestClassifier(
        labelCol="Severity",
        featuresCol="features",
        numTrees=50,
        maxBins=15000
    )

    pipeline = Pipeline(stages=indexers + [assembler, rf])

    logger.info("Splitting into train/test")
    train, test = df.randomSplit([0.8, 0.2], seed=42)

    logger.info("Training RandomForestClassifier")
    model = pipeline.fit(train)

    logger.info("Evaluating model")
    predictions = model.transform(test)

    evaluator = MulticlassClassificationEvaluator(
        labelCol="Severity", predictionCol="prediction", metricName="accuracy"
    )
    accuracy = evaluator.evaluate(predictions)
    logger.info(f"Model accuracy: {accuracy:.4f}")

    model_path = "ml_training/models/us_accidents_rf_model"
    logger.info(f"Saving model to {model_path}")
    model.write().overwrite().save(model_path)

except Exception as e:
    logger.error(f"Training error: {e}")
    raise

finally:
    spark.stop()

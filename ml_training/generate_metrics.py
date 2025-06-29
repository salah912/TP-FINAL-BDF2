import json
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

def main():
    spark = SparkSession.builder \
        .appName("Generate Evaluation Metrics") \
        .getOrCreate()

    # Load model
    model = PipelineModel.load("/app/models/us_accidents_rf_model")

    # Load test data
    df = spark.read.parquet("hdfs://namenode:9000/gold/top_20_cities")
    selected_cols = ["Start_Lat", "Start_Lng", "City", "State", "year", "month", "day", "Severity"]
    df = df.select(*selected_cols).dropna()

    # Split
    train, test = df.randomSplit([0.7, 0.3], seed=42)

    # Predict
    predictions = model.transform(test)

    # Evaluate
    evaluator = MulticlassClassificationEvaluator(
        labelCol="Severity",
        predictionCol="prediction",
        metricName="accuracy"
    )
    accuracy = evaluator.evaluate(predictions)

    # Confusion matrix as dict
    confusion_df = predictions.groupBy("Severity", "prediction").count().toPandas()
    confusion_matrix = confusion_df.to_dict(orient="records")

    # Save metrics to JSON
    metrics = {
        "accuracy": round(accuracy, 4),
        "confusion_matrix": confusion_matrix
    }

    with open("/app/ml_training/logs/metrics.json", "w") as f:
        json.dump(metrics, f, indent=4)

    spark.stop()

if __name__ == "__main__":
    main()

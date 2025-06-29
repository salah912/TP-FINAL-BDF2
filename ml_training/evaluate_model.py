from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.functions import col

def main():
    # Création de la session Spark
    spark = SparkSession.builder \
        .appName("Evaluate RF Model") \
        .getOrCreate()

    # Chargement du modèle
    model_path = "/app/models/us_accidents_rf_model"
    model = PipelineModel.load(model_path)

    # Chargement des données de test depuis le dossier Gold
    data_path = "hdfs://namenode:9000/gold/top_20_cities"
    df = spark.read.parquet(data_path)

    # Filtrage des colonnes nécessaires
    selected_cols = ["Start_Lat", "Start_Lng", "City", "State", "year", "month", "day", "Severity"]
    df = df.select(*selected_cols).dropna()

    # Division train/test (70/30)
    train, test = df.randomSplit([0.7, 0.3], seed=42)

    # Prédiction sur le jeu de test
    predictions = model.transform(test)

    # Évaluation du modèle
    evaluator = MulticlassClassificationEvaluator(
        labelCol="Severity", 
        predictionCol="prediction", 
        metricName="accuracy"
    )
    accuracy = evaluator.evaluate(predictions)
    print(f"Model accuracy: {accuracy:.4f}")

    # Affichage de la matrice de confusion
    predictions.groupBy("Severity", "prediction").count().orderBy("Severity", "prediction").show()

    spark.stop()

if __name__ == "__main__":
    main()

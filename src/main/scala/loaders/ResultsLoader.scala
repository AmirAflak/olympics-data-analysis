package loaders

import org.apache.spark.sql.{Dataset, Row, SparkSession}


class ResultsLoader(sparkSession: SparkSession) {

  def loadResults(): Dataset[Row] = sparkSession.read
    .format("parquet")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("datasets/results.parquet")
}



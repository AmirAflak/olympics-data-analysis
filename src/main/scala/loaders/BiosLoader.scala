package loaders

import org.apache.spark.sql.{Dataset, Row, SparkSession}


class BiosLoader(sparkSession: SparkSession) {

  def loadBios(): Dataset[Row] = sparkSession.read
    .format("parquet")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("datasets/bios.parquet")
}



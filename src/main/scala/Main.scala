import cleaners.BiosCleaner
import org.apache.spark.sql.{Dataset, Row, SparkSession}


object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("BiosCleaner")
      .master("local[2]")
      .config("spark.log.level", "ERROR")
      .getOrCreate()

    val bios = spark.read.format("parquet")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("/home/mehrdad/olympic-data-analysis/datasets/bios.parquet")

    val cleaner = new BiosCleaner(spark)
    val dfCleaned = cleaner.cleanBios(bios)

    dfCleaned.show(5)

    spark.stop()
  }
}



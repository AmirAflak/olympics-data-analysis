import org.apache.spark.sql.{Dataset, Row, SparkSession}
import ingestion.PostgresIngestor
import loaders.ResultsLoader
import loaders.BiosLoader
import cleaners.ResultsCleaner
import cleaners.BiosCleaner

object Main {
  def main(args: Array[String]) {
    val spark = SparkSession.builder
      .appName("Main")
      .master("local[2]")
      .config("spark.log.level", "ERROR")
      .config("spark.sql.catalogImplementation","hive")
      .getOrCreate()

    val resultsLoader = new ResultsLoader(spark)
    val resultsDF = resultsLoader.loadResults()

    val resultsCleaner = new ResultsCleaner(spark)
    val cleanedResultsDF = resultsCleaner.cleanResults(resultsDF)

    val postgresIngestor = new PostgresIngestor(spark)
    val postgresUrl = "jdbc:postgresql://localhost:5438/postgres"
    val postgresUser = "postgres"
    val postgresPassword = "postgres"
    val tableName = "results"

    postgresIngestor.ingest(cleanedResultsDF, tableName, postgresUrl, postgresUser, postgresPassword)
  }
}
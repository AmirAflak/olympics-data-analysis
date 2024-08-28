package cleaners

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.joda.time.format.DateTimeFormat
import org.joda.time.DateTime
import java.util.Locale
import java.sql.Date

class ResultsCleaner(sparkSession: SparkSession) {

  def cleanResults(df: Dataset[Row]): Dataset[Row] = {
    val dfWithPlaceAndTied = extractPlaceAndTied(df)
    val dfWithMedalValue = extractMedalValue(dfWithPlaceAndTied)
    val dfWithYearAndType = extractYearAndType(dfWithMedalValue)
    val dfWithYearAsInt = convertYearToInt(dfWithYearAndType)
    val dfSorted = sortResults(dfWithYearAsInt)
    val dfFiltered = filterColumns(dfSorted)
    val dfCleaned = renameColumns(dfFiltered)

    dfCleaned
  }

  private def extractPlaceAndTied(df: Dataset[Row]): Dataset[Row] = {
    df.withColumn("place", regexp_extract(col("Pos"), "(\\d+)", 1).cast("int"))
      .withColumn("tied", col("Pos").contains("="))
  }

  private def extractMedalValue(df: Dataset[Row]): Dataset[Row] = {
    df.withColumn("medal_value", when(col("Medal") === "Bronze", 3)
      .when(col("Medal") === "Silver", 2)
      .when(col("Medal") === "Gold", 1)
      .otherwise(null))
  }

  private def extractYearAndType(df: Dataset[Row]): Dataset[Row] = {
    df.withColumns(
      Map(
        "year" -> regexp_extract(col("Games"), "(\\d{4}) (Summer|Winter)", 1).cast("int"),
        "type" -> regexp_extract(col("Games"), "(\\d{4}) (Summer|Winter)", 2)
      )
    )
  }

  private def convertYearToInt(df: Dataset[Row]): Dataset[Row] = {
    df.withColumn("year", col("year").cast("int"))
  }

  private def sortResults(df: Dataset[Row]): Dataset[Row] = {
    df.sort(col("year").desc)
  }

  private def filterColumns(df: Dataset[Row]): Dataset[Row] = {
    val columnsToKeep = Seq("year", "type", "Discipline", "Event", "As", "athlete_id", "NOC", "Team", "place", "tied", "Medal")
    df.select(columnsToKeep.map(col): _*)
  }

  private def renameColumns(df: Dataset[Row]): Dataset[Row] = {
    df.columns.foldLeft(df)((acc, c) => acc.withColumnRenamed(c, c.toLowerCase.replace(" ", "_")))
  }
}

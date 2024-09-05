package cleaners

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.joda.time.format.DateTimeFormat
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import java.util.Locale
import java.sql.Date

class BiosCleaner(sparkSession: SparkSession) {

  def cleanBios(df: Dataset[Row]): Dataset[Row] = {
    val dfWithReplacedName = replaceName(df)
    val dfWithMeasurements = extractMeasurements(dfWithReplacedName)
    val dfWithCleanedMeasurements = cleanMeasurements(dfWithMeasurements)
    val dfWithParsedDates = parseDates(dfWithCleanedMeasurements)
    val dfWithDateTime = convertToDateTime(dfWithParsedDates)
    val dfWithTimestamp = convertToTimestamp(dfWithDateTime)
    val dfWithLocations = extractLocations(dfWithTimestamp)
    val dfCleaned = selectColumns(dfWithLocations)

    dfCleaned
  }

  private def replaceName(df: Dataset[Row]): Dataset[Row] = {
    df.withColumn("name", regexp_replace(col("Used name"), "•", " "))
  }

  private def extractMeasurements(df: Dataset[Row]): Dataset[Row] = {
    df.withColumn("height_cm", split(col("Measurements"), "/").getItem(0))
      .withColumn("weight_kg", split(col("Measurements"), "/").getItem(1))
  }

  private def cleanMeasurements(df: Dataset[Row]): Dataset[Row] = {
    df.withColumn("height_cm", regexp_replace(col("height_cm"), " cm", "").cast("double"))
      .withColumn("weight_kg", regexp_replace(col("weight_kg"), " kg", "").cast("double"))
  }

  private def parseDates(df: Dataset[Row]): Dataset[Row] = {
    val datePattern = "(\\d{1,2} [A-Za-z]+ \\d{4})"
    df.withColumn("born_date", regexp_extract(col("Born"), datePattern, 0))
      .withColumn("died_date", regexp_extract(col("Died"), datePattern, 0))
  }

private def convertToDateTime(df: Dataset[Row]): Dataset[Row] = {
  val parseDate = udf((date: String) => {
    if (date == null || date.trim.isEmpty) {
      null
    } else {
      try {
        val df = DateTimeFormat.forPattern("dd MMMM yyyy").withLocale(Locale.UK).withZone(DateTimeZone.UTC)
        val dt = DateTime.parse(date, df)
        new Date(dt.getMillis)
      } catch {
        case e: IllegalArgumentException =>
          try {
            val df = DateTimeFormat.forPattern("dd MMM yyyy").withLocale(Locale.US).withZone(DateTimeZone.UTC)
            val dt = DateTime.parse(date, df)
            new Date(dt.getMillis)
          } catch {
            case e: Exception =>
              null
          }
      }
    }
  })

  df.withColumn("born_date", parseDate(col("born_date")))
    .withColumn("died_date", parseDate(col("died_date")))
}

  private def convertToTimestamp(df: Dataset[Row]): Dataset[Row] = {
    df.withColumn("born_date", unix_timestamp(col("born_date"), "yyyy-MM-dd").cast("timestamp"))
      .withColumn("died_date", unix_timestamp(col("died_date"), "yyyy-MM-dd").cast("timestamp"))
  }

  private def extractLocations(df: Dataset[Row]): Dataset[Row] = {
    val locationPattern = """in ([\w\p{L}\s()-]+), ([\w\p{L}\s-]+) $$(\w+)$$"""
    df.withColumn("born_city", regexp_extract(col("Born"), locationPattern, 1))
      .withColumn("born_region", regexp_extract(col("Born"), locationPattern, 2))
      .withColumn("born_country", regexp_extract(col("Born"), locationPattern, 3))
  }

  private def selectColumns(df: Dataset[Row]): Dataset[Row] = {
    val columnsToKeep = Seq("athlete_id", "name", "born_date", "born_city", "born_region", "born_country", "NOC", "height_cm", "weight_kg", "died_date")
    df.select(columnsToKeep.map(col): _*)
  }
}


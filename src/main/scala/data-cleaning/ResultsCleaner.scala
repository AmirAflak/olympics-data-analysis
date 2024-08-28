import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.joda.time.format.DateTimeFormat
import org.joda.time.DateTime
import java.util.Locale
import java.sql.Date


object ResultsCleaner {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("BiosCleaner")
      .master("local[2]") 
      .config("spark.log.level", "ERROR")
      .getOrCreate()

    val results = spark.read.format("csv")
      .option("header", "true")
     .option("inferSchema", "true")
        .load("/home/mehrdad/olympic-data-analysis/datasets/results.csv")
    
    val dfWithPlaceAndTied = results
    .withColumn("place", regexp_extract(col("Pos"), "(\\d+)", 1).cast("int"))
    .withColumn("tied", col("Pos").contains("="))
    
        val dfWithMedalValue = dfWithPlaceAndTied.withColumn("medal_value", when(col("Medal") === "Bronze", 3)
    .when(col("Medal") === "Silver", 2)
    .when(col("Medal") === "Gold", 1)
    .otherwise(null))

    val dfWithYearAndType = dfWithMedalValue.withColumns(
    Map(
        "year" -> regexp_extract(col("Games"), "(\\d{4}) (Summer|Winter)", 1).cast("int"),
        "type" -> regexp_extract(col("Games"), "(\\d{4}) (Summer|Winter)", 2)
    )
    )

     val dfWithYearAsInt = dfWithYearAndType.withColumn("year", col("year").cast("int"))

     val dfSorted = dfWithYearAsInt.sort(col("year").desc)

     val columnsToKeep = Seq("year", "type", "Discipline", "Event", "As", "athlete_id", "NOC", "Team", "place", "tied", "Medal")
     val dfFiltered = dfSorted.select(columnsToKeep.map(col): _*)

     val dfCleaned = dfFiltered.columns.foldLeft(dfFiltered)((acc, c) => acc.withColumnRenamed(c, c.toLowerCase.replace(" ", "_")))
    
    dfCleaned.show(5)
    spark.stop()
  }
}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.joda.time.format.DateTimeFormat
import org.joda.time.DateTime
import java.util.Locale
import java.sql.Date


object BiosCleaner {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("BiosCleaner")
      .master("local[2]") 
      .config("spark.log.level", "ERROR")
      .getOrCreate()

    val bios = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("/home/mehrdad/olympic-data-analysis/datasets/bios.csv")

    val dfWithReplacedName = bios.withColumn("name", regexp_replace(col("Used name"), "•", " "))

    val dfWithMeasurements = dfWithReplacedName
      .withColumn("height_cm", split(col("Measurements"), "/").getItem(0))
      .withColumn("weight_kg", split(col("Measurements"), "/").getItem(1))

    val dfWithCleanedMeasurements = dfWithMeasurements
      .withColumn("height_cm", regexp_replace(col("height_cm"), " cm", "").cast("double"))
      .withColumn("weight_kg", regexp_replace(col("weight_kg"), " kg", "").cast("double"))

    val datePattern = "(\\d{1,2} [A-Za-z]+ \\d{4})"

    val dfWithParsedDates = dfWithCleanedMeasurements
      .withColumn("born_date", regexp_extract(col("Born"), datePattern, 0))
      .withColumn("died_date", regexp_extract(col("Died"), datePattern, 0))

    val parseDate = udf((date: String) => {
      if (date == null) {
        null
      } else {
        val df = DateTimeFormat.forPattern("dd MMMM yyyy").withLocale(Locale.US)
        val dt = DateTime.parse(date, df)
        new Date(dt.getMillis)
      }
    })


    val dfWithDateTime = dfWithParsedDates
      .withColumn("born_date", parseDate(col("born_date")))
      .withColumn("died_date", parseDate(col("died_date")))

    val dfWithTimestamp = dfWithDateTime
      .withColumn("born_date", unix_timestamp(col("born_date"), "yyyy-MM-dd").cast("timestamp"))
      .withColumn("died_date", unix_timestamp(col("died_date"), "yyyy-MM-dd").cast("timestamp"))

    val locationPattern = """in ([\w\p{L}\s()-]+), ([\w\p{L}\s-]+) \((\w+)\)"""

        val dfWithLocations = dfWithTimestamp
      .withColumn("born_city", regexp_extract(col("Born"), locationPattern, 1))
      .withColumn("born_region", regexp_extract(col("Born"), locationPattern, 2))
      .withColumn("born_country", regexp_extract(col("Born"), locationPattern, 3))

    val columnsToKeep = Seq("athlete_id", "name", "born_date", "born_city", "born_region", "born_country", "NOC", "height_cm", "weight_kg", "died_date")

    val dfCleaned = dfWithLocations.select(columnsToKeep.map(col): _*)
    
    dfCleaned.show(5)

    spark.stop()
  }
}
package ingestion

import org.apache.spark.sql.{DataFrame, SparkSession}

class PostgresIngestor(sparkSession: SparkSession) {

  def createTable(tableName: String, postgresUrl: String, postgresUser: String, postgresPassword: String, schema: String): Unit = {
    val createTableQuery = s"""
      CREATE TABLE IF NOT EXISTS $tableName (
        $schema
      );
    """

    sparkSession.read
      .format("jdbc")
      .option("url", postgresUrl)
      .option("user", postgresUser)
      .option("password", postgresPassword)
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", "(SELECT 1) AS temp_table")
      .load()

    sparkSession.sql(createTableQuery)
  }

  def writeTable(df: DataFrame, tableName: String, postgresUrl: String, postgresUser: String, postgresPassword: String): Unit = {
    df.write
      .format("jdbc")
      .option("url", postgresUrl)
      .option("user", postgresUser)
      .option("password", postgresPassword)
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", tableName)
      .save()
  }

  def ingest(df: DataFrame, tableName: String, postgresUrl: String, postgresUser: String, postgresPassword: String): Unit = {
    val schema = df.schema.fields.map(field => s"${field.name} ${field.dataType.simpleString}").mkString(", ")
    createTable(tableName, postgresUrl, postgresUser, postgresPassword, schema)
    writeTable(df, tableName, postgresUrl, postgresUser, postgresPassword)
  }
}
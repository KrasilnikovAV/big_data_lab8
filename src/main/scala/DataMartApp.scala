import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.current_timestamp

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.time.ZoneOffset
import java.util.UUID

object DataMartApp {
  private def env(name: String, default: String): String =
    sys.env.getOrElse(name, default)

  final case class Config(
      mysqlUrl: String = env("MYSQL_URL", "jdbc:mysql://mysql:3306/big_data_lab8?allowPublicKeyRetrieval=true&useSSL=false"),
      mysqlUser: String = env("MYSQL_USER", "lab8"),
      mysqlPassword: String = env("MYSQL_PASSWORD", "lab8"),
      mysqlDriver: String = env("MYSQL_DRIVER", "com.mysql.cj.jdbc.Driver"),
      sourceTable: String = env("MYSQL_SOURCE_TABLE", "product_nutrition"),
      martTable: String = env("MYSQL_MART_TABLE", "dm_product_nutrition"),
      refreshLogTable: String = env("MYSQL_REFRESH_LOG_TABLE", "dm_refresh_log"),
      outputDir: String = env("OUTPUT_DIR", "outputs/datamart"),
      sparkMaster: String = env("SPARK_MASTER", SparkConfig.DefaultSparkMaster),
      sparkConfigPath: String = env("SPARK_CONFIG_PATH", SparkConfig.DefaultSparkConfigPath),
      runId: String = buildRunId()
  )

  def main(args: Array[String]): Unit = {
    val config = parseArgs(args.toList, Config())
    val spark = buildSparkSession(config)

    try {
      run(config, spark)
    } finally {
      spark.stop()
    }
  }

  private def run(config: Config, spark: SparkSession): Unit = {
    import spark.implicits._

    val sourceDf = readJdbcTable(spark, config, config.sourceTable)
    val rowsInSource = DataMartTransformations.countSourceRows(sourceDf)
    val martDf = DataMartTransformations.transformToMart(sourceDf, config.sourceTable)
    val rowsInMart = martDf.count()
    val droppedRows = rowsInSource - rowsInMart

    writeJdbcTable(martDf, config, config.martTable, SaveMode.Overwrite)

    val refreshLogDf = Seq(
      (
        config.runId,
        config.sourceTable,
        config.martTable,
        rowsInSource,
        rowsInMart,
        droppedRows
      )
    ).toDF(
      "run_id",
      "source_table",
      "mart_table",
      "rows_in_source",
      "rows_in_mart",
      "dropped_rows"
    ).withColumn("created_at", current_timestamp())

    writeJdbcTable(refreshLogDf, config, config.refreshLogTable, SaveMode.Append)
    writeMetricsFile(config, rowsInSource, rowsInMart, droppedRows)

    println(s"Run ID: ${config.runId}")
    println(s"Source table: ${config.sourceTable}")
    println(s"Mart table: ${config.martTable}")
    println(s"Rows in source: $rowsInSource")
    println(s"Rows in mart: $rowsInMart")
    println(s"Dropped rows: $droppedRows")
  }

  private def buildSparkSession(config: Config): SparkSession = {
    SparkConfig.buildSparkSession(
      appName = "Lab7DataMart",
      master = config.sparkMaster,
      configPath = config.sparkConfigPath
    )
  }

  private def jdbcOptions(config: Config): Map[String, String] = Map(
    "url" -> config.mysqlUrl,
    "user" -> config.mysqlUser,
    "password" -> config.mysqlPassword,
    "driver" -> config.mysqlDriver
  )

  private def readJdbcTable(spark: SparkSession, config: Config, tableName: String): DataFrame =
    spark.read
      .format("jdbc")
      .options(jdbcOptions(config))
      .option("dbtable", tableName)
      .load()

  private def writeJdbcTable(df: DataFrame, config: Config, tableName: String, mode: SaveMode): Unit =
    df.write
      .format("jdbc")
      .options(jdbcOptions(config))
      .option("dbtable", tableName)
      .mode(mode)
      .save()

  private def writeMetricsFile(config: Config, rowsInSource: Long, rowsInMart: Long, droppedRows: Long): Unit = {
    Files.createDirectories(Paths.get(config.outputDir))
    val metricsPath = Paths.get(config.outputDir, "refresh_metrics.json")
    val json =
      s"""{
         |  "run_id": "${escapeJson(config.runId)}",
         |  "source_table": "${escapeJson(config.sourceTable)}",
         |  "mart_table": "${escapeJson(config.martTable)}",
         |  "rows_in_source": $rowsInSource,
         |  "rows_in_mart": $rowsInMart,
         |  "dropped_rows": $droppedRows,
         |  "created_at_utc": "${Instant.now()}"
         |}
         |""".stripMargin
    Files.write(metricsPath, json.getBytes(StandardCharsets.UTF_8))
  }

  private def parseArgs(args: List[String], defaults: Config): Config = {
    val setters: Map[String, (Config, String) => Config] = Map(
      "--mysql-url" -> ((config, value) => config.copy(mysqlUrl = value)),
      "--mysql-user" -> ((config, value) => config.copy(mysqlUser = value)),
      "--mysql-password" -> ((config, value) => config.copy(mysqlPassword = value)),
      "--mysql-driver" -> ((config, value) => config.copy(mysqlDriver = value)),
      "--source-table" -> ((config, value) => config.copy(sourceTable = value)),
      "--mart-table" -> ((config, value) => config.copy(martTable = value)),
      "--refresh-log-table" -> ((config, value) => config.copy(refreshLogTable = value)),
      "--output-dir" -> ((config, value) => config.copy(outputDir = value)),
      "--spark-master" -> ((config, value) => config.copy(sparkMaster = value)),
      "--spark-config" -> ((config, value) => config.copy(sparkConfigPath = value)),
      "--run-id" -> ((config, value) => config.copy(runId = value))
    )

    args match {
      case Nil => defaults
      case option :: value :: tail if setters.contains(option) =>
        parseArgs(tail, setters(option)(defaults, value))
      case option :: _ =>
        throw new IllegalArgumentException(s"Unknown or incomplete argument: $option")
    }
  }

  private def escapeJson(value: String): String =
    value.replace("\\", "\\\\").replace("\"", "\\\"")

  private def buildRunId(): String = {
    val timestamp = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'")
      .format(Instant.now().atOffset(ZoneOffset.UTC))
    s"$timestamp-${UUID.randomUUID().toString.take(8)}"
  }
}

import org.apache.spark.sql.SparkSession

object SparkConfig {
  val DefaultSparkMaster = "local[2]"
  val DefaultSparkConfigPath = "config/spark-defaults.conf"

  def buildSparkSession(
      appName: String,
      master: String = DefaultSparkMaster,
      configPath: String = DefaultSparkConfigPath,
      extraConfig: Map[String, String] = Map.empty
  ): SparkSession = {
    val builder = SparkSession.builder()
      .appName(appName)

    val configuredBuilder =
      if (master.nonEmpty) builder.master(master) else builder

    val spark = (loadSparkConfig(configPath) ++ extraConfig).foldLeft(configuredBuilder) {
      case (current, (key, value)) => current.config(key, value)
    }.getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark
  }

  def loadSparkConfig(configPath: String): Map[String, String] = {
    KeyValueConfig.load(configPath, "Spark config")
  }
}

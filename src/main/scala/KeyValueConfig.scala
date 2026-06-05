import scala.io.Source

object KeyValueConfig {
  def load(configPath: String, configName: String): Map[String, String] = {
    val source = Source.fromFile(configPath)
    try {
      source.getLines()
        .map(_.trim)
        .filter(line => line.nonEmpty && !line.startsWith("#"))
        .map { line =>
          val parts = line.split("=", 2)
          if (parts.length != 2) {
            throw new IllegalArgumentException(
              s"Invalid $configName line in '$configPath': $line"
            )
          }
          parts(0).trim -> parts(1).trim
        }
        .toMap
    } finally {
      source.close()
    }
  }
}


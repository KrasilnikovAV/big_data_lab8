import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, trim}

object OpenFoodFactsSchema {
  val DefaultSchemaConfigPath = "config/openfoodfacts-schema.conf"

  final case class FeatureRange(min: Double, max: Double)

  final case class SchemaDefinition(
      metaColumns: Seq[String],
      modelMetaColumns: Seq[String],
      featureColumns: Seq[String],
      numericMappings: Seq[(String, String)],
      featureRanges: Map[String, FeatureRange]
  ) {
    val requiredColumns: Seq[String] = metaColumns ++ featureColumns

    def martSelectedColumns: Seq[Column] =
      Seq(
        trim(col("product_code")).as("product_code"),
        trim(col("product_name")).as("product_name"),
        trim(col("brands")).as("brands")
      ) ++ featureColumns.map(name => col(name).cast("double").as(name))
  }

  def load(configPath: String = DefaultSchemaConfigPath): SchemaDefinition = {
    val entries = KeyValueConfig.load(configPath, "schema config")
    val featureColumns = parseList(required(entries, "feature_columns"))

    SchemaDefinition(
      metaColumns = parseList(required(entries, "meta_columns")),
      modelMetaColumns = parseList(required(entries, "model_meta_columns")),
      featureColumns = featureColumns,
      numericMappings = parseMappings(required(entries, "numeric_mappings")),
      featureRanges = featureColumns.map { feature =>
        feature -> parseRange(required(entries, s"range.$feature"))
      }.toMap
    )
  }

  private def parseList(value: String): Seq[String] =
    value.split(",").map(_.trim).filter(_.nonEmpty).toSeq

  private def parseMappings(value: String): Seq[(String, String)] =
    value.split(",").toSeq.filter(_.nonEmpty).map { pair =>
      val parts = pair.split(":", 2)
      if (parts.length != 2) {
        throw new IllegalArgumentException(s"Invalid mapping entry: $pair")
      }
      parts(0).trim -> parts(1).trim
    }

  private def parseRange(value: String): FeatureRange = {
    val parts = value.split(":", 2)
    if (parts.length != 2) {
      throw new IllegalArgumentException(s"Invalid range entry: $value")
    }
    FeatureRange(parts(0).trim.toDouble, parts(1).trim.toDouble)
  }

  private def required(entries: Map[String, String], key: String): String =
    entries.getOrElse(
      key,
      throw new IllegalArgumentException(s"Missing schema config key: $key")
    )
}

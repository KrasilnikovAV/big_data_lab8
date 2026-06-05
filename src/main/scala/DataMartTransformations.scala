import org.apache.spark.sql.Column
import org.apache.spark.sql.{DataFrame, functions => F}

object DataMartTransformations {
  private val schema = OpenFoodFactsSchema.load()
  private val featureColumns = schema.featureColumns
  private val requiredColumns = schema.requiredColumns

  def validateSourceColumns(df: DataFrame, tableName: String): Unit = {
    val missing = requiredColumns.diff(df.columns.toSeq).sorted
    if (missing.nonEmpty) {
      throw new IllegalArgumentException(
        s"Source table '$tableName' is missing columns: ${missing.mkString(", ")}"
      )
    }
  }

  def countSourceRows(sourceDf: DataFrame): Long =
    selectRelevantColumns(sourceDf).count()

  def transformToMart(sourceDf: DataFrame, sourceTable: String): DataFrame = {
    validateSourceColumns(sourceDf, sourceTable)

    selectRelevantColumns(sourceDf)
      .na.drop("any", featureColumns)
      .where(buildRangePredicate())
      .withColumn("source_table", F.lit(sourceTable))
      .withColumn("refreshed_at", F.current_timestamp())
  }

  private def selectRelevantColumns(sourceDf: DataFrame): DataFrame =
    sourceDf.select(schema.martSelectedColumns: _*)

  private def buildRangePredicate(): Column =
    featureColumns
      .map { feature =>
        val range = schema.featureRanges(feature)
        F.col(feature).between(range.min, range.max)
      }
      .reduce(_ && _)
}

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class DataMartTransformationsSpec extends AnyFunSuite with BeforeAndAfterAll {
  private val schemaConfig = OpenFoodFactsSchema.load()

  private val spark = SparkConfig.buildSparkSession(
    appName = "DataMartTransformationsSpec",
    master = "local[1]",
    extraConfig = Map(
      "spark.ui.enabled" -> "false",
      "spark.sql.shuffle.partitions" -> "1",
      "spark.default.parallelism" -> "1"
    )
  )

  override protected def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }

  test("transformToMart keeps only valid rows and enriches them with mart metadata") {
    val schema = StructType(
      schemaConfig.requiredColumns.map(name => StructField(name, StringType, nullable = true))
    )

    val input = spark.createDataFrame(
      spark.sparkContext.parallelize(
        Seq(
          Row(" 001 ", "  Valid product ", " Brand A ", "120.5", "10.0", "20.0", "5.0", "7.5", "1.2"),
          Row("002", "Too salty", "Brand B", "200.0", "11.0", "19.0", "6.0", "4.0", "25.0"),
          Row("003", "Missing energy", "Brand C", null, "11.0", "19.0", "6.0", "4.0", "0.4")
        )
      ),
      schema
    )

    val result = DataMartTransformations.transformToMart(input, "product_nutrition")
    val rows = result.select("product_code", "product_name", "brands", "source_table").collect()

    assert(result.count() == 1)
    assert(rows.length == 1)
    assert(rows.head.getString(0) == "001")
    assert(rows.head.getString(1) == "Valid product")
    assert(rows.head.getString(2) == "Brand A")
    assert(rows.head.getString(3) == "product_nutrition")
    assert(result.schema(schemaConfig.featureColumns.head).dataType == DoubleType)
    assert(result.filter("refreshed_at IS NULL").count() == 0)
  }

  test("validateSourceColumns fails when required columns are missing") {
    val missingFeature = schemaConfig.featureColumns.last
    val schema = StructType(
      schemaConfig.requiredColumns
        .filterNot(_ == missingFeature)
        .map(name => StructField(name, if (schemaConfig.metaColumns.contains(name)) StringType else DoubleType, nullable = true))
    )

    val input = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row("001", "Product", "Brand", 100.0, 10.0, 20.0, 5.0, 7.0))),
      schema
    )

    val error = intercept[IllegalArgumentException] {
      DataMartTransformations.validateSourceColumns(input, "product_nutrition")
    }

    assert(error.getMessage.contains(missingFeature))
  }
}

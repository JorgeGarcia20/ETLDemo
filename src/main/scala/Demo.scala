import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit, when}
import Schemas._

object Demo extends App {
  val spark = SparkSession.builder()
    .appName("ETLDemo")
    .master("local")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()

  val mortgagePath = "data/MORTGAGE.csv"
  val mortgageDf = spark.read.option("header", "true").schema(mortgageSchema).csv(mortgagePath)





  mortgageDfProcesado.show(false)

  // val data = spark.range(5)
  // data.show()
  // data.write.format("delta").save("bronze/tmp/test")
  // data.write.format("delta").mode("append").saveAsTable("data_test")
  // val dataParquet = spark.read.format("delta").load("bronze/tmp/test")
  // dataParquet.show()
  // spark.table("data_test").show()
}

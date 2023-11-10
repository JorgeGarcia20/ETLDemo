import Demo.{interestRatesDf, mortgageApplicantsDf, mortgageDf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit, split, when}
import Schemas._
import DataFrameUtils._

object Demo extends App {
  val spark = SparkSession.builder()
    .appName("ETLDemo")
    .master("local")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()

  val mortgagePath = "data/MORTGAGE.csv"
  val mortgageApplicantsPath = "data/MORTGAGE_APPLICANTS.csv"
  val interestRatesPath = "data/INTEREST_RATES.csv"
  val creditScorePath = "data/CREDIT_SCORE.csv"
  /*
  * Lectura de archivos .csv
  * */
  val mortgageDf = spark.read.option("header", "true").schema(mortgageSchema).csv(mortgagePath)
  val mortgageApplicantsDf = spark.read.option("header", "true").schema(mortgageApplicants).csv(mortgageApplicantsPath)
  val interestRatesDf = spark.read.option("header", "true").schema(interetRatesSchema).csv(interestRatesPath)
  val creditScoreDf = spark.read.option("header", "true").schema(creditScoreSchema).csv(creditScorePath)

  /*
  * Carga de bronces
  * */
  mortgageDf.write.format("delta").save("data/bronze/mortgage")
  mortgageApplicantsDf.write.format("delta").save("data/bronze/mortgage_applicants")
  interestRatesDf.write.format("delta").save("data/bronze/interest_rates")
  creditScoreDf.write.format("delta").save("data/bronze/credit_score")

  /*
  * Limpieza de dataframes
  * */

  val mortgageDfSilver = cleanMortgageDf(mortgageDf)
  val mortgageApplicantsDfCleaned = cleanMortgageApplicantsDf(mortgageApplicantsDf)
  val mortgageApplicantsDfSilver = mortgageApplicantsDfCleaned.join(creditScoreDf, Seq("ID"), "inner")
  // val interestRatesDfSilver = spark.read.format("delta").load("data/bronze/interest_rates")

  mortgageApplicantsDfSilver.show()

}

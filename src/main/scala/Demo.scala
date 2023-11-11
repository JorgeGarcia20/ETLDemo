// import Demo.{interestRatesDf, mortgageApplicantsDf, mortgageDf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit, split, when}
import Schemas._
import DataFrameUtils._
import Bank._

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

  val mortgageTable = "mortgage"
  val mortgageApplicantsTable = "mortgage_applicants"
  val interestRatesTable = "interest_rates"
  val creditScoreTable = "credit_score"

  // paso 1 compribacion de existencia de tablas.
  if(!spark.catalog.tableExists(mortgageApplicantsTable)){
    Bank.generarTablaApplicants(spark, mortgageApplicantsPath, mortgageApplicantsTable, "overwrite")
  }else if(!spark.catalog.tableExists(interestRatesTable)){
    Bank.generarTablaInterestRates(spark, interestRatesPath, interestRatesTable, "overwrite")
  }

  // spark.table(mortgageApplicantsTable).show(false)

  // paso 2 lectura de archivos mortgage.csv y credit_score.csv.
  val mortgageDf = spark.read.option("header", "true").schema(mortgageSchema).csv(mortgagePath)
  val creditScoreDf = spark.read.option("header", "true").schema(creditScoreSchema).csv(creditScorePath)

  // paso 3 carga de dataframes crudos a tablas delta.
  mortgageDf.write.format("delta").mode("overwrite").save("data/bronze/mortgage")
  creditScoreDf.write.format("delta").mode("overwrite").save("data/bronze/credit_score")

  // paso 4 limpieza de dataframes
  val mortgageDfSilver = cleanMortgageDf(mortgageDf)

  // paso 5 carga de dataframe limpio a tabla plata
  mortgageDfSilver.write.format("delta").mode("overwrite").save("data/silver/mortgage")

  // paso 6 generacion de dataframe oro

  val mortgageGold = mortgageDfSilver.join(spark.table(mortgageApplicantsTable), Seq("ID"), "inner")
    .join(creditScoreDf, Seq("ID"), "inner")

  // paso 7 carga de dataframe oro a tabla delta.
  mortgageDfSilver.write.format("delta").mode("overwrite").save("data/gold/mortgage")

  /*
  mortgageDfSilver.write.format("delta").save("data/silver/mortgage")
  val mortgageApplicantsDfGold = spark.read.format("delta").load("data/silver/mortgage_applicants")
    .join(spark.read.format("delta").load("data/silver/mortgage"), Seq("ID"), "inner")
    .join(creditScoreDf, Seq("ID"), "inner")

  mortgageApplicantsDfGold.show()
   */
}

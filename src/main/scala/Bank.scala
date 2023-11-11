import org.apache.spark.sql.SparkSession
import Schemas.{mortgageApplicants, creditScoreSchema, interetRatesSchema}
import org.apache.spark.sql.functions.{col, split, when}

object Bank {
  def generarTablaApplicants(spark: SparkSession, path: String, nombreTabla: String, mode: String): Unit = {
    val mortgageApplicantsDf = spark.read
      .option("header", "true")
      .schema(mortgageApplicants)
      .csv(path)
      .withColumn("LASTNAME", split(col("NAME"), " ")(1))
      .withColumn("NAME", split(col("NAME"), " ")(0))
      .withColumn("EXTERNAL_NUMBER", split(col("STREET_ADDRESS"), " ")(0))
      .withColumn("STREET_ADDRESS", split(col("STREET_ADDRESS"), " ")(1))
      .withColumn("EMPLOYMENT_STATUS",
        when(col("EMPLOYMENT_STATUS") === "Unemployed", "0")
          .otherwise("1")
      )
      .select("ID", "NAME", "LASTNAME", "STREET_ADDRESS", "EXTERNAL_NUMBER", "CITY",
        "STATE",
        "STATE_CODE",
        "EMAIL_ADDRESS",
        "PHONE_NUMBER",
        "GENDER",
        "SOCIAL_SECURITY_NUMBER",
        "EMPLOYMENT_STATUS"
      )

    mortgageApplicantsDf.write.format("delta").mode(mode).saveAsTable(nombreTabla)
  }

  /*
  def generarTablaCreditScore(spark: SparkSession, path: String, nombreTabla: String, mode: String): Unit = {
    spark.read
      .option("header", "true")
      .schema(creditScoreSchema)
      .csv(path)
      .write
      .format("delta")
      .mode(mode)
      .saveAsTable(nombreTabla)
  }
  */

  def generarTablaInterestRates(spark: SparkSession, path: String, nombreTabla: String, mode: String): Unit = {
    spark.read
      .option("header", "true")
      .schema(interetRatesSchema)
      .csv(path)
      .write
      .format("delta")
      .mode(mode)
      .saveAsTable(nombreTabla)
  }
}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, split, when}

object DataFrameUtils {

  /*
  * Esta funcion permitira generar plata
  * */
  def cleanMortgageDf(df: DataFrame): DataFrame = {
    val appliedOnline = "APPLIEDONLINE"
    df.withColumn(appliedOnline, when(col(appliedOnline) === "1", "YES")
        .when(col(appliedOnline) === "Y", "YES")
        .when(col(appliedOnline) === "True", "YES")
        .when(col(appliedOnline) === "0", "NO")
        .when(col(appliedOnline) === "N", "NO")
        .when(col(appliedOnline) === "False", "NO")
        .otherwise(col(appliedOnline))
      )
      .withColumn("LOAN_AMOUNT", when(col("LOANS") === 0, lit(0)).otherwise(col("LOAN_AMOUNT")))
  }

  /*
  * Funcion para generar plata del dataframe MortgageApplicants
  *
  * */
  def cleanMortgageApplicantsDf(df: DataFrame): DataFrame = {
    df.withColumn("LASTNAME", split(col("NAME"), " ")(1))
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
  }
}

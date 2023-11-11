import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions.{col, lit, split, when}

object DataFrameUtils {

  def readCsvFile(spark: SparkSession, path: String, schema: StructType): DataFrame = {
    spark.read.option("header", "true").schema(schema).csv(path)
  }

  def saveDataFrameAsParquet(df: DataFrame, tablePath: String, mode: String): Unit = {
    df.write.format("delta").mode(mode).save(tablePath)
  }

  def saveDataFrameAsTable(df: DataFrame, tableName: String, mode: String): Unit = {
    df.write.format("delta").mode(mode).saveAsTable(tableName)
  }

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

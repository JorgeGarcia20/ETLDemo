import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, when, lit}

object DataFrameUtils {

  def generateMortgageSilver(df: DataFrame): DataFrame = {
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
}

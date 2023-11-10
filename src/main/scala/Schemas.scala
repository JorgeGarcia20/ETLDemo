import org.apache.spark.sql.types._

object Schemas {
  val mortgageSchema = StructType(Seq(
    StructField("ID", IntegerType, nullable=false),
    StructField("INCOME", IntegerType, nullable=false),
    StructField("APPLIEDONLINE", StringType, nullable=false),
    StructField("RESIDENCE", StringType, nullable=false),
    StructField("YRS_AT_CURRENT_ADDRESS", IntegerType, nullable=false),
    StructField("YRS_WITH_CURRENT_EMPLOYER", IntegerType, nullable=false),
    StructField("NUMBER_OF_CARDS", IntegerType, nullable=false),
    StructField("CREDIT_DEBT", IntegerType, nullable=false),
    StructField("LOANS", IntegerType, nullable=false),
    StructField("LOAN_AMOUNT", IntegerType, nullable=false),
  ))

  val creditScoreSchema = StructType(Seq(
    StructField("CREDIT_SCORE", StringType, nullable = false),
    StructField("ID", IntegerType, nullable = false)
  ))

  val interetRatesSchema = StructType(Seq(
    StructField("ID", IntegerType, nullable = false),
    StructField("STARTING_LIMIT", IntegerType, nullable = false),
    StructField("ENDING_LIMIT", IntegerType, nullable = false),
    StructField("RATE", IntegerType, nullable = false)
  ))

  val mortgageApplicants = StructType(Seq(
    StructField("ID", IntegerType, nullable = false),
    StructField("NAME", IntegerType, nullable = false),
    StructField("STREET_ADDRESS", IntegerType, nullable = false),
    StructField("CITY", IntegerType, nullable = false),
    StructField("STATE", IntegerType, nullable = false),
    StructField("STATE_CODE", IntegerType, nullable = false),
    StructField("EMAIL_ADDRESS", IntegerType, nullable = false),
    StructField("PHONE_NUMBER", IntegerType, nullable = false),
    StructField("GENDER", IntegerType, nullable = false),
    StructField("SOCIAL_SECURITY_NUMBER", IntegerType, nullable = false),
    StructField("EMPLOYMENT_STATUS", IntegerType, nullable = false)
  ))
}

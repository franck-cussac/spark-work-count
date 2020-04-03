package xke.local

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object HelloWorld {

  val DEFAULT_CITIES_DATA_PATH = "src/main/resources/cities.csv";
  val DEFAULT_DEPARTMENT_DATA_PATH = "src/main/resources/departements-france.csv";
  val DEFAULT_PARQUET_OUT_PUT_PATH = "src/main/parquets/parquet-result"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("HelloWorld").master("local[*]").getOrCreate()

    if (args.length >= 3) {

      val result = readDataAndApplyTransformations(spark, args(0), args(1))
      result.write
        .partitionBy("code_region", "code_departement")
        .mode(SaveMode.Overwrite)
        .parquet(args(2))

      result.show()
    } else {

      val result = readDataAndApplyTransformations(spark, DEFAULT_DEPARTMENT_DATA_PATH, DEFAULT_CITIES_DATA_PATH)
      result.write
        .partitionBy("code_region", "code_departement")
        .mode(SaveMode.Overwrite)
        .parquet(DEFAULT_PARQUET_OUT_PUT_PATH)

      result.show()
    }

  }

  private def readDataAndApplyTransformations(spark: SparkSession, dataPathDepartment: String,  dataPathCities: String): DataFrame = {
    val dfDepartment = spark.read
      .option("delimiter", ",")
      .option("header", value = true)
      .option("inferSchema", "true")
      .csv(dataPathDepartment)

    val dfCities = spark.read
      .option("delimiter", ",")
      .option("header", value = true)
      .option("inferSchema", "true")
      .csv(dataPathCities)

    val dfDepartmentWithIntDepartment = dfDepartment
      .withColumn("code_departement", toIntegerUdf(dfDepartment("code_departement")))

    val dfCitiesWithIntDepartment = dfCities
      .withColumn("department_code", toIntegerUdf(dfCities("department_code")))

    val dfJoinDepartmentCities = joinCitiesAndDepartment(dfDepartmentWithIntDepartment, dfCitiesWithIntDepartment)

    val dfWithCodeDepartmentAvg = avgDepByReg(dfJoinDepartmentCities)

    renameColumn(dfWithCodeDepartmentAvg, "avg(code_departement)", "avg_dep")
  }

  def joinCitiesAndDepartment(dfDepartment: DataFrame, dfCities: DataFrame): DataFrame = {
    dfDepartment
      .join(dfCities, col("code_departement") === col("department_code"), "left_outer")
      .drop("department_code")
  }

  val avgDepByReg: DataFrame => DataFrame =
    (df: DataFrame) => df.groupBy("code_region", "nom_region", "code_departement").agg(avg("code_departement"))

  def renameColumn(dataFrame: DataFrame, oldName: String, newName: String): DataFrame = {
    dataFrame.withColumnRenamed(oldName, newName)
  }

  //Custom UDF
  def toInteger(input: String): Int = {input.filter(Character.isDigit).toInt}
  val toIntegerUdf: UserDefinedFunction = udf(toInteger _)

}

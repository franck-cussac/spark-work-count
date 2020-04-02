package xke.local

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object HelloWorld {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate()

    val parseToIntUdf: UserDefinedFunction = udf(parseToInt _)

    val dfDepartments = spark.read
      .option("delimiter", ",")
      .option("header", true)
      .option("inferSchema", true)
      .csv("src/main/resources/departements-france.csv")
        .withColumn("code_departement", parseToIntUdf(col("code_departement")))
    dfDepartments.show()

    val dfCities = spark.read
      .option("delemiter", ",")
      .option("header", true)
      .option("inferSchema", true)
      .csv("src/main/resources/cities.csv")
        .withColumn("department_code", parseToIntUdf(col("department_code")))
    dfCities.show()

    val dfWithAvg = HelloWorld.avgDepByReg(dfDepartments)
    dfWithAvg.show()

    val dfWithAvgAndName = HelloWorld.renameColumn(dfWithAvg, "avg_dep")
    dfWithAvgAndName.show()

    val dfJoined = HelloWorld.joinDf(dfDepartments, dfCities)
    dfJoined.show()

    HelloWorld.writeParquet(dfJoined)
  }

  def parseToInt(s: String): Int =
    s.filter(Character.isDigit).toInt

  def avgDepByReg(df: DataFrame): DataFrame =
    df.groupBy("code_region", "nom_region")
      .agg(avg("code_departement"))

  def renameColumn(df: DataFrame, newName: String): DataFrame =
    df.withColumnRenamed("avg(code_departement)", newName)

  def joinDf(dfDepartements: DataFrame, dfCities: DataFrame): DataFrame =
    dfDepartements.join(dfCities, dfDepartements("code_departement") === dfCities("department_code"), "left_outer")
    .drop("department_code")

  def writeParquet(df: DataFrame): Unit =
    df.write.partitionBy("code_region", "code_departement")
      .mode("overwrite")
      .parquet("src/main/parquets/file.parquet")
}

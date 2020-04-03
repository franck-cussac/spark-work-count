package xke.local

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object HelloWorld {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    val toIntegerUdf: UserDefinedFunction = udf(parseInteger _)

    val df = spark.read.option("sep", ",")
      .option("header", true)
      .csv("src/main/resources/departements-france-short.csv")
      .withColumn("code_departement",toIntegerUdf(col("code_departement")))
    val dfCities = spark.read.option("header", true).csv("src/main/resources/cities.csv")


    val avg = avgDepByReg(df)
    val rename = renameColumn(avg)
    rename.show()
    writeToParquet(rename)

    val dfJoin = joinDf(df,dfCities)
    writeJoinToParquet(dfJoin)


  }

  def avgDepByReg(input: DataFrame): DataFrame = {
    input
      .groupBy("code_region", "nom_region")
      .agg(avg("code_departement"))
  }

  def renameColumn(input: DataFrame): DataFrame = {
    input
      .withColumnRenamed("avg(code_departement)", "avg_dep")
  }

  def writeToParquet(input: DataFrame)  = {
    input.write.mode("overwrite")
      .partitionBy("code_region","code_departement")
      .parquet("src/main/parquet")
  }

  def writeJoinToParquet(input: DataFrame)  = {
    input.write.mode("overwrite")
      .parquet("src/main/parquet/ex1.parquet")
  }

  def parseInteger(s: String): Int = {
    s.filter(Character.isDigit).toInt
  }

  def joinDf(dfDepartements: DataFrame,dfCities: DataFrame): DataFrame = {
    dfDepartements.join(dfCities,dfDepartements("code_departement") === dfCities("department_code"))
  }
}
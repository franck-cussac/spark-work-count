package xke.local

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object HelloWorld {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate()

    // Exo1
    val dep_reg = exo1(spark)
    // -- Exo1

    //Exo2
    val df = spark.read.option("sep", ",").option("header", true).csv("src/main/resources/cities.csv")
      .withColumnRenamed("department_code", "code_departement")
    val join = joinWithUdf(df, dep_reg, "code_departement")
    join.show()
    // -- Exo2
  }

  def joinWithUdf(df: DataFrame, dep_reg: DataFrame, colName: String) = {
    df
      .withColumn(
        colName,
        stringToIntUdf(df(colName)).as(colName))
      .join(
        dep_reg.withColumn(colName, stringToIntUdf(dep_reg(colName)).as(colName))
        , colName)
  }

  def exo1(spark: SparkSession): DataFrame = {
    val df = spark.read.option("sep", ",").option("header", true).csv("src/main/resources/departements-france.csv")
      .withColumn("code_departement", stringToIntUdf(col("code_departement")).as("code_departement"))

    val avg = avgDepByReg(df)
    val rename = renameColumn(avg)
    writeToParquet(rename)

    df
  }

  def stringToIntUdf = udf(stringToInt _)
  def stringToInt(input: String): Int = {
    input.filter(Character.isDigit).toInt
  }

  def avgDepByReg(input: DataFrame): DataFrame = {
    input
      .groupBy("code_region", "nom_region")
      .agg(avg("code_departement")/*, first("nom_region").as("nom_region")*/)
  }

  def renameColumn(input: DataFrame): DataFrame = {
    input
      .withColumnRenamed("avg(code_departement)", "avg_dep")
  }

  def writeToParquet(input: DataFrame)  = {
    input.write.mode("overwrite").parquet("src/main/parquet/ex1.parquet")
  }
}

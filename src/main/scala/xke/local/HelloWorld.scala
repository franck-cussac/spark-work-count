package xke.local

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object HelloWorld {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate()

    val df = spark.read.option("sep", ",").option("header", true).csv("src/main/resources/departements-france-short.csv")
    val avg = avgDepByReg(df)
    val rename = renameColumn(avg)
    rename.show()
    writeToParquet(rename)
  }

  def avgDepByReg(input: DataFrame): DataFrame = {
    return input
      .groupBy("code_region", "nom_region")
      .agg(avg("code_departement")/*, first("nom_region").as("nom_region")*/)
  }

  def renameColumn(input: DataFrame): DataFrame = {
    return input
      .withColumnRenamed("avg(code_departement)", "avg_dep")
  }

  def writeToParquet(input: DataFrame)  = {
    input.write.mode("overwrite").parquet("src/main/parquet/ex1.parquet")
  }
}
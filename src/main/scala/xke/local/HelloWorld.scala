package xke.local

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object HelloWorld {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate()

    val dframe = spark.read.option("sep", ",").option("header", true).csv("src/main/resources/departements-france.csv")
    val average = avgDepByReg(dframe)
    val renameCol = renameColumn(average)
    renameCol.show()
    writeToParquet(renameCol)
  }

  def avgDepByReg(df: DataFrame): DataFrame = {
    return df
      .groupBy("code_region", "nom_region")
      .agg(avg("code_departement"))
  }

  def renameColumn(df: DataFrame): DataFrame = {
    return df
      .withColumnRenamed("avg(code_departement)", "avg_dep")
  }

  def writeToParquet(df: DataFrame)  = {
    df.write.mode("overwrite").parquet("src/main/parquet/ex1.parquet")
  }
}
package xke.local

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object HelloWorld {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("").getOrCreate()
    val df = spark.read.option("delimiter", ",").option("header", true).csv("src/main/resources/departements-france.csv")

    val dfIntColumn = df.withColumn("code_departement", col("code_departement").cast(IntegerType))
    val newDF = avgDepByReg(dataFrame = dfIntColumn)
    val newDfColumn = newColumn(dataFrame = newDF)
    val newDfName = renameColumn(dataFrame = newDfColumn)

    newDfName.write.mode(SaveMode.Overwrite)parquet("ParquetResult")
    spark.read.parquet("ParquetResult").show
  }

  def avgDepByReg(dataFrame: DataFrame): DataFrame = {
    dataFrame.groupBy(col("code_region")).avg("code_departement").as("avg_dep")
  }

  def renameColumn(dataFrame: DataFrame): DataFrame = {
    dataFrame.withColumnRenamed("avg(code_departement)","avg_dep")
  }

  def newColumn(dataFrame: DataFrame): DataFrame = {
    dataFrame.withColumn("average", col("avg(code_departement)"))
  }
}
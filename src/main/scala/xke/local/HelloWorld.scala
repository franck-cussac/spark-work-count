package xke.local

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}



object HelloWorld {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("example of SparkSession").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    SparkSession.builder()

    val df = spark.read.option("delimiter", ",").option("header", true).csv("src/main/resources/departements-france.csv")
    val newDF = avgDepByReg(dataFrame = df)
    val newDfName = renameColumn(dataFrame = newDF)
    newDfName.show()
  }

  def avgDepByReg(dataFrame: DataFrame): DataFrame = {
    dataFrame.withColumn("code_departement", col("code_departement").cast("integer"))
      .groupBy(col("code_region")).
      avg("code_departement")
  }

  def renameColumn(dataFrame: DataFrame): DataFrame = {
    dataFrame.withColumnRenamed("avg(code_departement)","avg_dep")
  }

}

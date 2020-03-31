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
    val newDfName = renameColumn(dataFrame = newDF, originalString = "avg(code_departement)", newName = "avg_dep")
    newDfName.show()
    newDfName.write.parquet("C:/Users/Bristouflex/Desktop/Projets/spark-work-count/src/main/resources/toto")
  }

  def avgDepByReg(dataFrame: DataFrame): DataFrame = {
    dataFrame.groupBy("code_region", "nom_region").agg(avg("code_departement"))
  }

  def renameColumn(dataFrame: DataFrame, originalString: String, newName: String): DataFrame = {
    dataFrame.withColumnRenamed(originalString,newName)
  }

}

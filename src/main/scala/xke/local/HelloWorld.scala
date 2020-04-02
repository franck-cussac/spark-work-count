package xke.local

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


import org.apache.spark.sql._


object HelloWorld {
  def main(args: Array[String]): Unit = {
    jointureDepartVille()

  }

  def jointureDepartVille(): Unit = {
    val spark = SparkSession.builder().master("local").appName("example of SparkSession").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    SparkSession.builder()

    val dfDepart = spark.read.option("delimiter", ",").option("header", true).csv("src/main/resources/departements-france.csv")
    val dfVilles = spark.read.option("delimiter", ",").option("header", true).csv("src/main/resources/cities.csv")
    val dfJoined = joinDepart(dfDepart, dfVilles)
    dfJoined.show()

  }

  def joinDepart(dfDepart: DataFrame, dfVilles: DataFrame): DataFrame ={
    dfDepart.join(dfVilles,
      dfDepart("code_departement") === dfVilles("department_code"),
      "left_outer"
    )
      .drop("department_code")
  }

  def departementExercises(): Unit ={
    val spark = SparkSession.builder().master("local").appName("example of SparkSession").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    SparkSession.builder()

    val df = spark.read.option("delimiter", ",").option("header", true).csv("src/main/resources/departements-france.csv").withColumn("code_departement", udfConvertToInt(col("code_departement")))
    df.show()
    val newDF = avgDepByReg(dataFrame = df)
    newDF.show()
    val newDfName = renameColumn(dataFrame = newDF, originalString = "avg(code_departement)", newName = "avg_dep")
    newDfName.show()
    newDfName.write.mode("overwrite").parquet("src/main/resources/toto")
  }

  def udfConvertToInt: UserDefinedFunction = udf(convertToInt)

  def convertToInt: String => Int = (value: String) => value.filter(Character.isDigit).toInt


  def avgDepByReg(dataFrame: DataFrame): DataFrame = {
    dataFrame.groupBy("code_region", "nom_region").agg(avg("code_departement"))
  }

  def renameColumn(dataFrame: DataFrame, originalString: String, newName: String): DataFrame = {
    dataFrame.withColumnRenamed(originalString,newName)
  }

}

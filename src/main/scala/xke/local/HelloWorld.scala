package xke.local

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}





object HelloWorld {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("example of SparkSession").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    SparkSession.builder()
    jointureDepartVille(spark)
    //departementExercises(spark)
  }

  def jointureDepartVille(spark: SparkSession): Unit = {

    val dfDepart = spark.read.option("delimiter", ",").option("header", true).csv("src/main/resources/departements-france.csv")
    val dfVilles = spark.read.option("delimiter", ",").option("header", true).csv("src/main/resources/cities.csv")
    val dfJoined = doJoinOnDepartCode(dfDepart, dfVilles)
    dfJoined.show()
    writeInPartition(dfJoined, "code_region", "code_departement")
  }

  def doJoinOnDepartCode(dfDepart: DataFrame, dfVilles: DataFrame): DataFrame ={
    dfDepart.join(dfVilles,
      dfDepart("code_departement") === dfVilles("department_code"),
      "left_outer"
    )
      .drop("department_code")
  }

  def writeInPartition(toWrite: DataFrame, col1: String, col2: String): Unit = {
    toWrite
      .write
      .partitionBy(col1, col2)
      .mode("overwrite")
      .parquet("src/main/resources/joinPartition")
  }

  def departementExercises(spark: SparkSession): Unit ={

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

package xke.local

import com.sun.org.apache.xpath.internal.functions.FuncSubstringAfter
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import scala.util.Try

object HelloWorld {

  def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess

  def main(args: Array[String]): Unit = {
    /*val spark = SparkSession.builder().getOrCreate()
    i
    val inputFile = args(0)
    val outputFile = args(1)
    val input =  spark.sparkContext.textFile(inputFile)
    val words = input.flatMap(line => line.split(" "))
    val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
    counts.toDF("word", "count").write.mode(SaveMode.Overwrite).parquet(outputFile)*/

  //spark://spark-master:7077
    //.master("local[*]").appName("test")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df = spark.read.option("sep", ",").option("header", true).csv("src/main/resources/departements-france.csv")
    val dfVille = spark.read.option("sep", ",").option("header", true).csv("src/main/resources/cities.csv")
    val dfDepartments = renameColumn(spark.read.option("sep", ",").option("header", true).csv("src/main/resources/departments.csv"), "code", "department_code")
    val dfRegions = renameColumn(spark.read.option("sep", ",").option("header", true).csv("src/main/resources/regions.csv"), "code", "region_code")

    firstCSV(df, "src/main/resources/output/avg.parquet")
    partitionParquet(secondCSV(dfVille, dfDepartments, dfRegions), "src/main/resources/output/cities.parquet")
  }

  def firstCSV(df: DataFrame, output: String)= {
    val cleanedDF = cleanDep(df, "code_departement")
    writeToParquet(renameColumn(avgDepByReg(cleanedDF), "avg(code_departement)", "avg_dep"), output)
  }

  def secondCSV(dfVille: DataFrame, dfDepartments: DataFrame, dfRegions: DataFrame): DataFrame= {
    val joinedDf = dfVille.join(dfDepartments, dfVille.col("department_code")===dfDepartments.col("department_code")).drop(dfDepartments.col("department_code"))
    val finalDF = joinedDf.join(dfRegions, joinedDf.col("region_code")===dfRegions.col("region_code")).drop(dfRegions.col("region_code"))
    val cleanedFinalDF = cleanDep(finalDF, "department_code")
    return cleanedFinalDF

  }

  def partitionParquet(df: DataFrame, output: String): Unit ={
    df.write.mode("overwrite").partitionBy("region_code", "department_code").parquet(output)
  }

  def avgDepByReg(input: DataFrame): DataFrame = {
    return input
      .groupBy(col("code_region")).agg(
      avg(col("code_departement")),
      max("nom_region").as("nom_region")
    )
  }
  def renameColumn(input: DataFrame, before: String, after: String): DataFrame = {
    return input
      .withColumnRenamed(before, after)
  }
  def writeToParquet(input: DataFrame, output: String) = {
    input.write.mode("overwrite").parquet(output)
  }

  def cleanDep (input: DataFrame, column: String): DataFrame = {
    val extract: UserDefinedFunction = udf(udfZero _)
    if(hasColumn(input, column)) return input.withColumn(column,extract(col(column)) as column )
    else return input
  }

  def udfZero(input: String): Int = {
    input match {
      case x if x.startsWith("0") => return x.substring(1).toInt
      case "2A" => return 2
      case "2B" => return 2
      case _ => return input.toInt
    }
  }
}

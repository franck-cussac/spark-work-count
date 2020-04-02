package xke.local

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object HelloWorld {



  def main(args: Array[String]): Unit = {
    /*val spark = SparkSession.builder().getOrCreate()
    i
    val inputFile = args(0)
    val outputFile = args(1)
    val input =  spark.sparkContext.textFile(inputFile)
    val words = input.flatMap(line => line.split(" "))
    val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
    counts.toDF("word", "count").write.mode(SaveMode.Overwrite).parquet(outputFile)*/


    val spark = SparkSession.builder().master("local[*]").appName("test").getOrCreate()
    import spark.implicits._

    val df = spark.read.option("sep", ",").option("header", true).csv("src/main/resources/departements-france.csv")/*
      .filter(col("code_region") % 2 === 0)
      .groupBy(col("code_region")).count()
      .filter(col("count") > 5)*/



    renameColumn(avgDepByReg(df)).show()
    writeToParquet(renameColumn(avgDepByReg(df)), "src/main/resources/output/avg.parquet")

  }

  def avgDepByReg(input: DataFrame): DataFrame = {
    return input
      .groupBy(col("code_region")).agg(
      avg(col("code_departement")),
      max("nom_region").as("nom_region")
    )
  }
  def renameColumn(input: DataFrame): DataFrame = {
    return input
      .withColumnRenamed("avg(code_departement)", "avg_dep")
  }
  def writeToParquet(input: DataFrame, output: String) = {
    input.write.mode("overwrite").parquet(output)
  }

  def cleanDep (input: DataFrame): DataFrame = {
    val extract: UserDefinedFunction = udf(udfZero _)
    return input.withColumn("code_departement",extract(col("code_departement")) )
  }

  def udfZero(input: String): Int = {
    input match {
      case x if x.startsWith("0") => return x.substring(1).toInt
      case "2A" => return 2
      case "2B" => return 2
    }
  }
}

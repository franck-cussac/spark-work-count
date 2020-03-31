package xke.local

import com.sun.org.apache.xalan.internal.xsltc.compiler.util.IntType
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object HelloWorld {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("").getOrCreate()

    /*import spark.implicits._

    val inputFile = args(0)
    val outputFile = args(1)
    val input =  spark.sparkContext.textFile(inputFile)
    val words = input.flatMap(line => line.split(" "))
    val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
    counts.toDF("word", "count").write.mode(SaveMode.Overwrite).parquet(outputFile)*/

    // 1) lire le fichier
    // 2) créer une colonne avec la moyenne des numéro département par code région
    //    code_region, avg_dep, nom_region
    // 3) renommer la colonne moyenne des départements en avg_dep
    // 4) écrire le fichier en parquet

    val df = spark.read.option("delimiter", ",").option("header", true).csv("src/main/resources/departements-france.csv")

    val dfIntColumn = df.withColumn("code_departement", col("code_departement").cast(IntegerType))
    val newDF = avgDepByReg(dataFrame = dfIntColumn)
    val newDfColumn = newColumn(dataFrame = newDF)
    val newDfName = renameColumn(dataFrame = newDfColumn)

    writeParquet(dataFrame = newDfName)

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

  def writeParquet(dataFrame: DataFrame){
    dataFrame.write.mode(SaveMode.Overwrite).parquet("ParquetResult")
  }
}

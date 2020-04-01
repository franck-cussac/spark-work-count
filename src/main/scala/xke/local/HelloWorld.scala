package xke.local

import com.sun.org.apache.xalan.internal.xsltc.compiler.util.IntType
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType


object HelloWorld {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("").getOrCreate()

    // 1) lire le fichier
    // 2) créer une colonne avec la moyenne des numéro département par code région
    //    code_region, avg_dep, nom_region
    // 3) renommer la colonne moyenne des départements en avg_dep
    // 4) écrire le fichier en parquet
    var path = "src/main/resources/departements-france.csv"
    if(args.length > 0) {
      path = args(0)
    }

    val df = spark.read.option("delimiter", ",").option("header", true).csv(path)
    val dfIntColumn = df.withColumn("code_departement", col("code_departement").cast(IntegerType))
    val newDF = avgDepByReg(dataFrame = dfIntColumn)
    val newDfName = renameColumn(dataFrame = newDF)

    writeToParquet(dataFrame = newDfName, pathToFile = "ParquetResult")
    printParquet(spark ,pathToFile = "ParquetResult")
  }

  def avgDepByReg(dataFrame: DataFrame): DataFrame = {
    dataFrame.groupBy(col("code_region"), col("nom_region")).avg("code_departement").as("avg_dep")
  }

  def renameColumn(dataFrame: DataFrame): DataFrame = {
    dataFrame.withColumnRenamed("avg(code_departement)","avg_dep")
  }

  def writeToParquet(dataFrame: DataFrame, pathToFile: String): Unit = {
    dataFrame.write.mode(SaveMode.Overwrite).parquet(pathToFile)
  }

  def printParquet(spark: SparkSession, pathToFile: String) {
    spark.read.parquet(pathToFile).show
  }


}
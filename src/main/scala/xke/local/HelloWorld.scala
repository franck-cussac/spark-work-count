package xke.local

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object HelloWorld {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("Salut Monde").getOrCreate()

    // 1) lire le fichier
    // 2) créer une colonne avec la moyenne des numéro département par code région
    //    code_region, avg_dep, nom_region
    // 3) renommer la colonne moyenne des départements en avg_dep
    // 4) écrire le fichier en parquet
    val df = spark.read.option("delimiter", ",").option("header", value = true).csv("src/main/resources/departements-france.csv")

    val newDF = getAverageDepartmentNumberByRegion(dataFrame = df)
    val newDfName = renameAverageColumn(dataFrame = newDF)
    newDfName.show
  }

  def getAverageDepartmentNumberByRegion(dataFrame: DataFrame): DataFrame = {
    dataFrame.groupBy(col("code_region")).avg("code_departement").as("avg_dep")
  }

  def renameAverageColumn(dataFrame: DataFrame): DataFrame = {
    dataFrame.withColumnRenamed("average","avg_dep")
  }
}
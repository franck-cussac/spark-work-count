package xke.local

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object HelloWorld {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    // src/main/resources/departements-france.csv
    // 1) lire le fichier
    // 2) créer une colonne avec la moyenne des numéro département par code région
    //    code_region, avg_dep, nom_region
    // 3) renommer la colonne moyenne des départements en avg_dep
    // 4) écrire le fichier en parquet

    val df = spark.read.option("delimiter", ",").option("header", value = true).csv("src/main/resources/departements-france-small.csv")
    val newDF = getAverageDepartmentNumberByRegion(df)
    val newDfName = renameAverageColumn(newDF)
    newDfName.show()
    writeToParquet(newDfName)
  }

  def getAverageDepartmentNumberByRegion(input: DataFrame): DataFrame = {
    input.groupBy("code_region", "nom_region")
      .agg(avg("code_departement"))
  }

  def renameAverageColumn(input: DataFrame): DataFrame = {
    input.withColumnRenamed("avg(code_departement)", "avg_dep")
  }

  def writeToParquet(input: DataFrame): Unit = {
    input.write.mode("overwrite").parquet("src/main/parquet/exo.parquet")
  }
}

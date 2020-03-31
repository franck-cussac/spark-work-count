package xke.local

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object HelloWorld {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate()

    // code
    // src/main/resources/departements-france.csv
    // 1) lire le fichier
    // 2) créer une colonne avec la moyenne des numéro département par code région
    //    code_region, avg_dep, nom_region
    // 3) renommer la colonne moyenne des départements en avg_dep
    // 4) écrire le fichier en parquet

    // 1
    val df = spark.read.option("sep", ",").option("header", true).csv("src/main/resources/departements-france.csv")

    // 2
    val dfAvg = avgDepByReg(df)

    // 3
    val dfRename = renameColumn(dfAvg, "avg_departement", "avg_dep")

    // 4
    write(dfRename, "C:\\Users\\christopher\\Downloads\\sort\\file.parquet")

    // Display
    dfRename.show

  }

  def avgDepByReg(dataFrame: DataFrame): DataFrame = {
    dataFrame.withColumn("code_departement", dataFrame.col("code_departement")
      .cast("Float"))
      .groupBy(col("code_region"))
      .agg(avg("code_departement").as("avg_departement"),
           first("nom_region").as("nom_region"))
  }

  def renameColumn(dataFrame: DataFrame, theOldName: String, theNewName: String): DataFrame = {
    dataFrame.withColumnRenamed(theOldName, theNewName)
  }

  def write(dataFrame: DataFrame, path: String) = {
    dataFrame.write
      .parquet(path)
  }

}


package xke.local

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object HelloWorld {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("").getOrCreate()

    // 1) lire le fichier
    // 2) créer une colonne avec la moyenne des numéro département par code région
    //    code_region, avg_dep, nom_region
    // 3) renommer la colonne moyenne des départements en avg_dep
    // 4) écrire le fichier en parquet

    val df = spark.read.option("header", true).csv("src/main/resources/departements-france.csv")
      .withColumn("moyenne",col = col("code_region").cast("integer"))

    val newDF = avgDepByReg(dataFrame = df)
    val newDfName = renameColumn(dataFrame = newDF)
    newDfName.show
  }

  def avgDepByReg(dataFrame: DataFrame): DataFrame = {
    dataFrame.groupBy("code_region", "nom_region")
      .agg(avg("code_departement"))
   // dataFrame.groupBy(col("code_region")).avg("code_region").as("avg_dep")
  }

  def renameColumn(dataFrame: DataFrame): DataFrame = {
    dataFrame.withColumnRenamed("avg(code_departement)","avg_dep")
  }
}
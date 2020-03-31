package xke.local

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

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


    val df = spark.read.option("delimiter", ",").option("header", true).csv("src/main/resources/departements-france.csv")


    avgDepByReg(df)
    df.write.parquet("fichiers_dep")
  }

  def avgDepByReg(df: DataFrame): DataFrame = {
    df.withColumn("code_region", col("avg_dep") + col("nom_region"))
      .groupBy(col("code_region"))
      .avg("code_departement").as("avg_dep").show()
    return df
  }
  def renameColumn(df: DataFrame): DataFrame = {
    return df
  }
}
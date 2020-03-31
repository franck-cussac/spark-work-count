package xke.local

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object HelloWorld {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate()
    val df = spark.read.option("header", true).csv("src/main/resources/departements-france.csv")
      .groupBy(col("code_region"))
      .avg(col("code_departement").cast("integer"))
      .as("avg_dep")
      .show()

    // code
    // src/main/resources/departements-france.csv
    // 1) lire le fichier
    // 2) créer une colonne avec la moyenne des numéro département par code région
    //    code_region, avg_dep, nom_region
    // 3) renommer la colonne moyenne des départements en avg_dep
    // 4) écrire le fichier en parquet

  }

  def avgDepByReg: DataFrame = ???
  def renameColumn: DataFrame = ???
}

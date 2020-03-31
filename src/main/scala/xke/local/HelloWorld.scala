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

    val df = spark.read.option("sep", ",").option("header", true).csv("C:\\hadoop\\project\\spark-work-count\\src\\main\\resources\\departements-france.csv")
    val avg = avgDepByReg(df)
    avg.show

    val renamed = renameColumn(avg, "avg_dep", "avg_departement")
    renamed.show
  }

  def avgDepByReg(dataFrame: DataFrame): DataFrame = {
    dataFrame.withColumn("code_departement", dataFrame.col("code_departement")
      .cast("Double"))
      .groupBy(col("code_region"))
      .agg(avg("code_departement").as("avg_departement"),max("nom_region").as("nom_region"))
  }

  def renameColumn(dataFrame: DataFrame, newName: String, oldName: String): DataFrame = {
    dataFrame.withColumnRenamed(oldName, newName)
  }
}

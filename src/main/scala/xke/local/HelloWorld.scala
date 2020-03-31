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
    val df = spark.read.option("header", true).csv("src/main/resources/departements-france.csv")
    /*  .filter(col("code_region") % 2 === 0)
      .groupBy(col("code_region")).count()
      .filter(col("count") >= 5)*/
    //df.write.parquet("output.parquet")
    //avgDepByReg(df).show

    val newDf = renameColumn(avgDepByReg(df))

  }

  def avgDepByReg(df: DataFrame): DataFrame = {
    df.groupBy(col("code_region"))
      .avg("code_departement")
      .as("avg_dep")
  }
  def renameColumn(df: DataFrame): DataFrame = {
    df.withColumnRenamed("average", "avg_dep")
  }
}

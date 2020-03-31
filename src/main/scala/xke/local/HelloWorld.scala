package xke.local

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object HelloWorld {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("MIVI").master("local[*]").getOrCreate()

    // code
    // src/main/resources/departements-france.csv
    // 1) lire le fichier
    // 2) créer une colonne avec la moyenne des numéro département par code région
    //    code_region, avg_dep, nom_region
    // 3) renommer la colonne moyenne des départements en avg_dep
    // 4) écrire le fichier en parquet

    val df = spark.read.option("header", "true")
      .option("delimiter", ",")
      .csv("src/main/resources/departements-france.csv")
//      .withColumn("code_departement", col("code_departement").cast("integer"))
//      .groupBy("code_region")
//      .avg("code_departement").as("avg_dep")
//      .show()

    val df2 = avgDepByReg(dataFrame = df)
    val df3 = renameColumn(dataFrame = df2)
    df3.show()

  }
  def avgDepByReg(dataFrame: DataFrame): DataFrame = {
    dataFrame.groupBy("code_region")
      .avg("code_departement").as("avg_dep")
  }

  def renameColumn(dataFrame: DataFrame): DataFrame = {
    dataFrame.withColumnRenamed("average","avg_dep")
  }
}

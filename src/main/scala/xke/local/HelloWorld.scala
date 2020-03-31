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

    val df = spark.read.option("header", "true").option("delimiter", ",").csv("src/main/resources/departements-france.csv")
    val df2 = avgDepByReg(df)
    val df3 = renameColumn(df2)

    df3.show()

  }

  def avgDepByReg(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .withColumn("code_departement", col("code_departement").cast("integer"))
      .groupBy("code_region", "nom_region").avg("code_departement")
  }
  def renameColumn(dataFrame: DataFrame): DataFrame = {
    dataFrame.withColumnRenamed("average", "avg_dep")
  }
}

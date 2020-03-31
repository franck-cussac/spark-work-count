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
      .withColumn("code_departement", col("code_departement").cast("integer"))
    renameColumn(avgDepByReg(df)).show
  }

  def avgDepByReg(df: DataFrame): DataFrame = {
    df.groupBy("code_region", "nom_region")
      .agg(avg("code_departement"))
  }
  def renameColumn(df: DataFrame): DataFrame = {
    df.withColumnRenamed("avg(code_departement)", "avg_dep")
  }
}

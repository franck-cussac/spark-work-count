package xke.local

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object HelloWorld {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    // 1) lire le fichier
    // 2) créer une colonne avec la moyenne des numéro département par code région
    //    code_region, avg_dep, nom_region
    // 3) renommer la colonne moyenne des départements en avg_dep
    // 4) écrire le fichier en parquet

    val df = spark.read.option("header", true).csv("src/main/resources/departements-france.csv")
      .withColumn("moyenne",col = col("code_region").cast("integer"))

    val dfAvg = avgDepByReg(df = df)
    val dfRenameC = renameColumn(df = dfAvg)

   // df.write.mode("overwrite").parquet("src/main/resources/parquet/ex1/")
    dfRenameC.write.mode("overwrite").parquet("src/main/parquet/ex1")


    //val dfParquet = writePaquet(df = dfRenameC)
    dfRenameC.show
  }

  def avgDepByReg(df: DataFrame): DataFrame = {
    df.groupBy("code_region", "nom_region")
      .agg(avg("code_departement"))
  }

  def renameColumn(df: DataFrame): DataFrame = {
    df.withColumnRenamed("avg(code_departement)","avg_dep")
  }


}
package xke.local

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object HelloWorld {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate() // ctrl + Q affiche le type de la variable

    // code
    // src/main/resources/departements-france.csv
    // 1) lire le fichier
    // 2) créer une colonne avec la moyenne des numéro département par code région
    //    code_region, avg_dep, nom_region
    // 3) renommer la colonne moyenne des départements en avg_dep
    // 4) écrire le fichier en parquet

    val df = createDateFrame(spark)
    val avg_dep_df = avgDepByReg(df)
    val result_df = renameColumn(avg_dep_df)

    result_df.write.parquet("/home/ubuntu/workspace/hadoop/spark-work-count/src/main/resources/parquet")

  }

  def createDateFrame(sparkSession: SparkSession): DataFrame = {
    sparkSession.read.option("header", true).csv("src/main/resources/departements-france.csv")
  }

  def avgDepByReg(dataFrame: DataFrame): DataFrame = {
    dataFrame.groupBy("code_region", "nom_region")
      .agg(avg("code_departement"))
  }

  def renameColumn(dataFrame: DataFrame): DataFrame = {
    dataFrame.withColumnRenamed("avg(code_departement)", "avg_dep")
  }
}

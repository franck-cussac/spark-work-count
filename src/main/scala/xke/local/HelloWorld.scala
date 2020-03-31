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

    val df = spark.read.option("delimiter", ",").option("header", true).csv("src/main/resources/departements-france.csv")
    val exportPath = "src/main/resources/myOutput.parquet"

    start(spark, df, exportPath)
  }

  def start(spark: SparkSession, df: DataFrame, exportPath: String) : Unit = {

    val dfCasted = df.withColumn("code_departement", col("code_departement").cast("integer"))

    val dfAvg = avgDepByReg(dfCasted)
    val dfRenamed = renameColumn(dfAvg)

    dfRenamed.write.mode(SaveMode.Overwrite).parquet(exportPath)
  }

  def avgDepByReg(dataFrame: DataFrame) : DataFrame = {
    dataFrame.groupBy("code_region", "nom_region")
      .agg(avg("code_departement"))
      .select("code_region", "avg(code_departement)", "nom_region")
  }

  def renameColumn(dataFrame: DataFrame) : DataFrame = {
    dataFrame.withColumnRenamed("avg(code_departement)", "avg_dep")
  }
}

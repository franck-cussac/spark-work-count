package xke.local

import org.apache.spark.sql.{DataFrame, _}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.expressions.UserDefinedFunction

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

    val avg = avgDepByReg(df)

    avg.show()

  }

  def avgDepByReg(dataFrame: DataFrame): DataFrame = {

    dataFrame.groupBy("code_region", "nom_region").agg(avg("code_departement"))
  }
  def renameColumn(dataFrame: DataFrame, oldC : String, newC : String): DataFrame = {
    dataFrame.withColumnRenamed(oldC, newC)
  }

  def writeParquet(dataFrame: DataFrame): Unit = {
    dataFrame.write.mode("overwrite").parquet("src/main/parquets/output.parquet")
  }
}

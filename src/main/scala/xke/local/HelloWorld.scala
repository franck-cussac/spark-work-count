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

    var df = spark.read.option("delimiter", ",").option("header", true).csv("src/main/resources/departements-france.csv")
    df = df.withColumn("code_departement", col("code_departement"))
        //.map(d => d.getString("code_departement"))
    df.show();
  }

  def avgDepByReg: DataFrame = ???
  def renameColumn: DataFrame = ???
}

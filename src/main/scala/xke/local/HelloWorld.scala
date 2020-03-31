package xke.local

import org.apache.spark.sql.{DataFrame, _}
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


  }

  def avgDepByReg(dataFrame: DataFrame): DataFrame = {

    dataFrame.groupBy(col("code_region"), col("nom_region")).avg("code_departement")
  }
  def renameColumn(dataFrame: DataFrame, oldC : String, newC : String) = {
    dataFrame.col(oldC).as(newC)
  }
}

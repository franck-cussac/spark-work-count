package xke.local

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object HelloWorld {
  def main(input: String): Unit = {
    val spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate()

    // code
    // src/main/resources/departements-france.csv
    // 1) lire le fichier
    // 2) créer une colonne avec la moyenne des numéro département par code région
    //    code_region, avg_dep, nom_region
    // 3) renommer la colonne moyenne des départements en avg_dep
    // 4) écrire le fichier en parquet
    val df = spark.read.option("sep",",").option("header",true  ).csv(input)
    avgDepByReg(df).show()
  }

  def avgDepByReg(df: DataFrame)  = {
    df.withColumn("code_departement", col("code_departement").cast("Integer"))
      .groupBy(df("code_region"),df("nom_region"))
      .avg("code_departement")


  }
  def renameColumn: DataFrame = ???
}

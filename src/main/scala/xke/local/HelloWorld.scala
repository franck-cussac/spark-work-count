package xke.local

import org.apache.spark.sql._
import org.apache.spark.sql.functions._


object HelloWorld {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("test")
      //.master("local[*]")
      .getOrCreate()
    import spark.implicits._


    // code
    // src/main/resources/departements-france.csv
    // 1) lire le fichier
    // 2) créer une colonne avec la moyenne des numéro département par code région
    //    code_region, avg_dep, nom_region
    // 3) renommer la colonne moyenne des départements en avg_dep
    // 4) écrire le fichier en parquet

    val df = spark.read.option("delimiter", ",").option("header", true).csv("src/main/resources/departements-france.csv")
    val df_avg = avgDepByReg(df)
    val df_renamed = renameColumn(df_avg, "avg(code_departement)", "avg_dep")
    df_renamed.show
    df_renamed.write.mode("overwrite").parquet("src/main/resources/output.parquet")
  }

  def avgDepByReg(df : DataFrame): DataFrame = {
    df
      .groupBy(col("code_region"), col("nom_region"))
      .agg(
        avg("code_departement")
      )
  }
  def renameColumn(df : DataFrame, oldName : String, newName : String): DataFrame = {
    df.withColumnRenamed(oldName, newName)
  }
}

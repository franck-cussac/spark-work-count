package xke.local

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object HelloWorld {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("").getOrCreate()

    //src/main/resources/departement-france.csv
    // Lire le fichier
    // Garder les codes regions pairs
    // Regrouper les regions par dizaine de code region
    // Garder que les lignes avec 10 résultats de regions dans la dizaine
    // Afficher le nombre de lignes finales

    /*val df = spark.read.option("delimiter", ",").option("header", true).csv("src/main/resources/departements-france.csv")
        .filter(col("code_region") % 2 === 0)
        .groupBy(col("code_region")).count()
        .filter(col("count") > 5)

    df.cache()

    println(df.count())
    val list = df.collect()
    println(list.length)*/

    val df = spark.read.parquet("output.parquet");

    //df.write.parquet("")

    df.withColumn("test", col("region_id") + col("code_region"))
      .write.mode(SaveMode.Overwrite).parquet("result")

    spark.read.parquet("result")

    // lire le fichier
    // creer une colonne avec la moyenne des numeros departeemnt par code region
    // renommer la colonne moyenne des departements en avg dep
    // ecrire le fichier en parquet
  }

  def avgDepByReg: DataFrame = ???
  def renameColumn: DataFrame = ???
}

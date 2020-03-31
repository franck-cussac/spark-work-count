package xke.local

import org.apache.spark.sql.{SaveMode, SparkSession}

object HelloWorld {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("test")getOrCreate()

    //src/main/resources/departements-france.csv
    // lire fichier
    // garder les codes regions pair
    // regrouper les régions par dizaine de code région
    // garder que les lignes avec 10 résultat
    // afficher le nombre de lignes finale

    val df = spark.read.option("delimiter", ",").option("header", true).csv("src/main/resources/departements-france.csv")

    df.filter(df("code_region") % 2 === 0).groupBy(df("code_region")).count().show()


    spark.sparkContext.setLogLevel("ERROR")
  }
}

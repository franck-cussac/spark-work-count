package xke.local

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object HelloWorld {
  def main(args: Array[String]): Unit = {
    /*val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val inputFile = args(0)
    val outputFile = args(1)
    val input =  spark.sparkContext.textFile(inputFile)
    val words = input.flatMap(line => line.split(" "))
    val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
    counts.toDF("word", "count").write.mode(SaveMode.Overwrite).parquet(outputFile)*/

    val spark = SparkSession.builder().master("local[*]").appName("test").getOrCreate()
    //src/main/resources/departements-france.csv
    //1 lire
    //2 code region paire
    //3 regrouper région par dizaine : 1 ligne 20aine avec liste région
    //4 garder ligne avec 10 résultats
    //5 afficher nb ligne final

    val df = spark.read.option("sep", ",").option("header", true).csv("src/main/resources/departements-france.csv")/*
      .filter(col("code_region") % 2 === 0)
      .groupBy(col("code_region")).count()
      .filter(col("count") > 5)

    df.show
    println(df.count())
    df.cache()

    println(s"1 :  ${df.collect.length}")*/

    //df.write.parquet("output.parquet")
    //lire fichier
    //groupby code region
    //colonne moyenne des numéros departement (traitement string)
    //  code_region, avg_dep, nom_region
    //renommer colonne mooyenne des départements
    //eccrire fichier en parquet

    renameColumn(avgDepByReg(df)).show()



  }

  def avgDepByReg(input: DataFrame): DataFrame = {
    return input
      .groupBy(col("code_region")).agg(
      avg(col("code_departement")),
      max("nom_region").as("nom_region")
    )
  }
  def renameColumn(input: DataFrame): DataFrame = {
    return input
      .withColumnRenamed("avg(code_departement)", "avg_dep")
  }
}

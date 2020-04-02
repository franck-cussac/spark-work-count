package xke.local

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object HelloWorld {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Angelo").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    /*val df = spark.read.option("delimiter", ",").option("header", true).csv("src/main/resources/departements-france.csv")
      .filter(col("code_region") % 2 === 0)
      .groupBy(col("code_region")).count()
      .filter(col("count" )> 5)

     */
    val df = spark.read.option("delimiter", ",").option("header", true).csv("src/main/resources/departements-france.csv")
    // code
    // val stringToIntUdf: UserDefinedFunction = udf(stringToInt(_))

    // src/main/resources/departements-france.csv
    // 1) lire le fichier
    // 2) créer une colonne avec la moyenne des numéro département par code région
    //    code_region, avg_dep, nom_region
    // 3) renommer la colonne moyenne des départements en avg_dep
    // 4) écrire le fichier en parquet
    spark.udf.register("STRINGTOINT", stringToInt _);
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
  def stringToInt  (input: String) : Int = {
    if (input.startsWith("0")) {
      input.substring(1).toInt
    }
    if(input.endsWith("A") || input.endsWith("B")){
      input.substring(0,input.length-1).toInt
    }
    else{
      input.toInt
    }
  }
}

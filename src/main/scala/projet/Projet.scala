package projet.Projet

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object Projet {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Projet").master("local[*]").getOrCreate()
    val df = spark.read.option("header", true).csv("src/main/resources/df_matches.csv")


  }

}

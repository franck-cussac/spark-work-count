package esgi.exo.FootballApp

import org.apache.spark.sql.SparkSession

object FootballApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("FootballApp").master("local[*]").getOrCreate()
    val dfMatches = spark.read.option("header", true).csv("src/main/resources/df_matches.csv")

    dfMatches.show

  }
}

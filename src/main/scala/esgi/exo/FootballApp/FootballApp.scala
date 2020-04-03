package esgi.exo.FootballApp

import org.apache.spark.sql.{DataFrame, SparkSession}

object FootballApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("FootballApp").master("local[*]").getOrCreate()
    val dfMatches = spark.read.option("header", true).csv("src/main/resources/df_matches.csv")

    val dfMatchesClean = cleanDF(dfMatches)
    dfMatchesClean.show

  }


  def cleanDF(dataFrame: DataFrame) : DataFrame = {
    dataFrame
      .withColumnRenamed("X4","match")
      .withColumnRenamed("X6","competition")
      .select("match","competition","adversaire","score_france","score_adversaire","penalty_france","penalty_adversaire","date")
  }
}

package esgi.exo

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object FootballApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("FootballApp").master("local[*]").getOrCreate()

    val df = spark.read.option("sep", ",")
      .option("header", true)
      .csv("src/main/resources/df_matches.csv")

    val cleanDf = cleanData(df)
        .withColumn("domicile", isMatchAtHomeUdf(col("match")))

    avgGoalFrance(cleanDf)
      .join(avgGoalOpponent(cleanDf),"adversaire")
      .join(countMatchPlayed(cleanDf),"adversaire")
      .join(percentPlayedAtHome(cleanDf),"adversaire")
      .join(countMatchesWorldCup(cleanDf),"adversaire")
      .join(maxFrancePenality(cleanDf),"adversaire")
      .join(calculPenaltyDifference(cleanDf),"adversaire")
      .write
        .mode("overwrite")
        .parquet("src/main/parquet/stats.parquet")

      val stats = spark.sqlContext.read.parquet("src/main/parquet/stats.parquet");

      joinCleanStats(cleanDf, stats, "src/main/parquet/result.parquet")
  }

  def joinCleanStats(clean: DataFrame, stats: DataFrame, path: String): Unit = {
    clean
      .join(stats, "adversaire")
      .withColumn("year", col("date").substr(0, 4))
      .withColumn("month", col("date").substr(6, 2))
      .write
      .partitionBy("year", "month")
      .mode("overwrite")
      .parquet(path)
  }

  def cleanData(df: DataFrame): DataFrame = {
      df
        .withColumnRenamed("X4", "match")
        .withColumnRenamed("X6", "competition")
        .select("match",
            "competition",
            "adversaire",
            "score_france",
            "score_adversaire",
            "penalty_france",
            "penalty_adversaire",
            "date")
        .withColumn("penalty_france", when(col("penalty_france").equalTo("NA"), "0").otherwise(col("penalty_france")))
        .withColumn("penalty_adversaire", when(col("penalty_adversaire").equalTo("NA"), "0").otherwise(col("penalty_france")))
        .filter(col("date") >= "1980-01-01")
        .filter(length(col("date")) === 10)
  }

  def avgGoalFrance(df: DataFrame): DataFrame = {
    df
      .groupBy("adversaire")
      .agg(avg(col("score_france")).as("avg_france_score"))
  }

  def avgGoalOpponent(df: DataFrame): DataFrame = {
    df
      .groupBy("adversaire")
      .agg(avg(col("score_adversaire")).as("avg_adversaire_score"))
  }

  def countMatchPlayed(df: DataFrame): DataFrame = {
    df
      .groupBy("adversaire")
      .agg(count("match").as("match_count"))
  }

  def percentPlayedAtHome(df: DataFrame): DataFrame = {
    df
      .groupBy("adversaire")
      .agg(count(when(col("domicile") ===  true, 1)).as("percentage_domicile"))
  }

  def countMatchesWorldCup(df: DataFrame): DataFrame = {
    df
      .groupBy("adversaire")
      .agg(count(when(col("competition").startsWith("Coupe du monde"), 1)).as("count_world_cup"))
  }

  def maxFrancePenality(df: DataFrame): DataFrame = {
    df
      .groupBy("adversaire")
      .agg(max("penalty_adversaire").as("max_penalty_france"))
  }

  def calculPenaltyDifference(df: DataFrame): DataFrame = {
    df
      .groupBy("adversaire")
      .agg((sum("penalty_adversaire") - sum("penalty_france")).as("penalty_diff"))
  }

  def isMatchAtHomeUdf= udf(isMatchAtHome _)
  def isMatchAtHome(duel: String): Boolean = {
      val arr = duel.split("-")
      if(arr(0) == "France ") {
          return true
      }

      false
  }
}

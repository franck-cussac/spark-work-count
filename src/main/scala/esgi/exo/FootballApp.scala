package esgi.exo

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object FootballApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("FootballApp").master("local[*]").getOrCreate()

    val df = spark.read.option("sep", ",").option("header", true).csv("src/main/resources/df_matches.csv")
    val cleanedDf = cleanDf(df)
    cleanedDf.cache()
    cleanedDf.show
    avgOpponentStat(cleanedDf).show

    val statsParquet = spark.read.parquet("src/main/data/match_stats/")

    val joinedDf = cleanedDf.withColumnRenamed("adversaire","adversaire_drop")
      .join(statsParquet, col("adversaire_drop") === statsParquet.col("adversaire"), "inner")
      .drop("adversaire_drop")
    joinedDf.show
  }

  def cleanDf(dataFrame: DataFrame): DataFrame ={
    dataFrame.filter(col("date") > "1980-00-00")
      .withColumnRenamed("X4","match")
      .withColumnRenamed("X6","competition")
      .select("match", "competition", "adversaire", "score_france", "score_adversaire", "penalty_france", "penalty_adversaire", "date")
      .withColumn("penalty_france", when(col("penalty_france").equalTo("NA"), "0").otherwise(col("penalty_france")))
      .withColumn("penalty_adversaire", when(col("penalty_adversaire").equalTo("NA"), "0").otherwise(col("penalty_adversaire")))
  }

  def avgOpponentStat(dataFrame: DataFrame): DataFrame = {
    val isHome = (s: String) => {s.startsWith("France -")}
    val homeUdf = udf(isHome)

    val stats = dataFrame.withColumn("domicile", homeUdf(col("match")) as "domicile")
      .groupBy(col("adversaire"))
      .agg(
        avg("score_france").as("avg_france_score_by_match"),
        avg("score_adversaire").as("avg_opponent_score_by_match"),
        count("score_france").as("total_match"),
        count(when(col("domicile"), 1)).as("home_game"),
        count(when(col("competition").startsWith("Coupe du monde"), 1)).as("world_cup_game"),
        max("penalty_france").as("greatest_number_penalty"),
        (sum(col("penalty_france")) - sum(col("penalty_adversaire"))).as("penalty_difference")
      )
      .withColumn("home_percentage", (col("home_game")/col("total_match")) * 100)

    stats.write.mode("overwrite").parquet("src/main/data/match_stats/")
    stats
  }
}

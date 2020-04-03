package esgi.exo

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sources.IsNull

object FootballApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("FootballApp").master("local[*]").getOrCreate()

    val correctionPenaltyUdf: UserDefinedFunction = udf(correctionPenalty _)
    val atHomeUdf: UserDefinedFunction = udf(atHome _)
    val isWorldCupUdf: UserDefinedFunction = udf(isWorldCup _)

    val dfMatchs = spark.read
      .option("delimiter", ",")
      .option("header", true)
      .option("inferSchema", true)
      .csv("src/main/resources/df_matches.csv")
        .withColumnRenamed("X4", "match")
        .withColumnRenamed("X6", "competition")
        .select("match", "competition", "adversaire", "score_france", "score_adversaire", "penalty_france", "penalty_adversaire", "date")
        .withColumn("penalty_france", correctionPenaltyUdf(col("penalty_france")))
        .withColumn("penalty_adversaire", correctionPenaltyUdf(col("penalty_adversaire")))
        .filter(to_date(col("date")).geq(lit("1980-03-01")))
        .withColumn("atHome", atHomeUdf(col("match")))
        .withColumn("isWorldCup", isWorldCupUdf(col("competition")))
        .withColumn("month", month(col("date")))
        .withColumn("year", year(col("date")))
    dfMatchs.show()

    val dfStatsMatchs = dfMatchs.groupBy("adversaire").agg(
      avg("score_france").as("Moyenne_score_France"),
      avg("score_adversaire").as("Moyenne_score_Adversaire"),
      count("match").as("Nb_matchs_joués"),
      count(when(col("atHome"), true)).divide(count("match")).multiply(100).as("pourcent_match_domicile"),
      count(when(col("isWorldCup"), true)).as("Matchs_World_Cup"),
      max(col("penalty_france")).as("Max_penaltys_France"),
      sum(col("penalty_france")).minus(sum(col("penalty_adversaire"))).as("Différence_pénaltys")
    )
    dfStatsMatchs.show()

    val dfJoined = FootballApp.joinDf(dfMatchs, dfStatsMatchs)
    dfJoined.show()

    FootballApp.writeParquet(dfJoined)
  }

  def correctionPenalty(s: String): Int =
    s match {
      case "NA" => 0
      case "null" => 0
      case _ => s.toInt
    }

  def atHome(s: String): Boolean =
    s.substring(0, 6).toLowerCase() match {
      case "france" => true
      case _ => false
    }

  def isWorldCup(s: String): Boolean =
    s match {
      case null => false
      case _ => s.toLowerCase.startsWith("coupe du monde")
    }

  def writeParquet(df: DataFrame): Unit =
    df.write.partitionBy("year", "month")
      .mode("overwrite")
      .parquet("src/main/resources/stats.parquet")

  def joinDf(dfMatchs: DataFrame, dfStatsMatchs: DataFrame): DataFrame =
    dfMatchs.join(dfStatsMatchs, dfMatchs("adversaire") === dfStatsMatchs("adversaire"), "left_outer")
    .drop(dfMatchs("adversaire"))

}
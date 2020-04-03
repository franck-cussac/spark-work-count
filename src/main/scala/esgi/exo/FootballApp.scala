package esgi.exo

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object FootballApp {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").appName("Futebooooooool").getOrCreate()

    //Nettoyer les datas
    //Calculer des stats
    //Joindre
    val dfMatches = sparkSession.read.option("delimiter", ",").option("header", value = true).csv("src/main/resources/df_matches.csv")
    val dfMatchesRenamed = dfMatches.withColumnRenamed("X4", "match").withColumnRenamed("X6", "competition")
    val dfMatchesSelect = dfMatchesRenamed.select("match", "competition", "adversaire", "score_france", "score_adversaire", "penalty_france", "penalty_adversaire", "date")

    val manageNullPenaltiesUDF = udf(manageNullPenalties _)
    val dfMatchesNullPenalties = dfMatchesSelect
      .withColumn("penalty_france", manageNullPenaltiesUDF(col("penalty_france")))
      .withColumn("penalty_adversaire", manageNullPenaltiesUDF(col("penalty_adversaire")))

    val dfMatchesAfter = dfMatchesNullPenalties.filter(to_date(col("date")).geq(lit("1980-03-01")))

    val homeUDF = udf(home _)
    val dfMatchPlayedHome = dfMatchesAfter.withColumn("home", homeUDF(col("match")))

    val dfMatchesStats = dfMatchPlayedHome
      .groupBy(col("adversaire"))
      .agg(
        avg("score_france").as("average_france"),
        avg("score_adversaire").as("average_adversaire"),
        count("match").as("total_match"),
        count(when(col("home"), true))
          .divide(count("match"))
          .multiply(100).as("percent_play_home"),
        count(when(col("competition").startsWith("Coupe du monde"), col("competition"))).as("total_match_world_cup"),
        max("penalty_adversaire").as("max_penalty_adversaire"),
        sum("penalty_adversaire").minus(sum("penalty_france")).as("difference_penalty")
      )
    dfMatchesStats.show()
    dfMatchesStats.write.mode("overwrite").parquet("src/main/parquet/exo.parquet")
  }

  def manageNullPenalties(point: String): Int = {
    point match {
      case "NA" => 0
      case null => 0
      case _ => point.toInt
    }
  }

  def home(_match: String): Boolean = {
    _match match {
      case null => false
      case _ => _match.toLowerCase().startsWith("france")
    }
  }
}
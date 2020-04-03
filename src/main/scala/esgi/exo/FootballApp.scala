package esgi.exo

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object FootballApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("football-app").getOrCreate()
    import spark.implicits._

    val fillPenaltyUdf = udf(fillPenalty _)
    val isAtHomeUdf = udf(isAtHome _)
    val isInWorldCupUdf = udf(isInWorldCup _)

    /**
     * Partie 1 : nettoyer les données
     */
    val df_matches_fromScratch = spark.read
      .option("delimiter", ",")
      .option("header", true)
      .option("inferSchema", true)
      .csv("src/main/resources/df_matches.csv")

    val df_matches_renamedColumns = renameColumns(
      df_matches_fromScratch,
      Map(
        "X4" -> "match",
        "X6" -> "competition"
      )
    )

    val df_matches_selectedColumns = df_matches_renamedColumns.select("match", "competition", "adversaire", "score_france", "score_adversaire", "penalty_france", "penalty_adversaire", "date")

    val df_matches_fillPenalty = df_matches_selectedColumns
      .withColumn("penalty_france", fillPenaltyUdf(col("penalty_france")))
      .withColumn("penalty_adversaire", fillPenaltyUdf(col("penalty_adversaire")))

    val df_matches_afterMarch1980 = filterByDateGeq(df_matches_fillPenalty, "date", "1980-03-01")

    df_matches_afterMarch1980.persist()

    /**
     * Partie 2 : calculer des statistiques
     */
    val df_matches_home = df_matches_afterMarch1980.withColumn("home", isAtHomeUdf(col("match")))

    val df_matches_stats = df_matches_home
      .groupBy(col("adversaire"))
      .agg(
        avg("score_france").as("average_france"),
        avg("score_adversaire").as("average_adversaire"),
        count("match").as("total_match"),
        count(when(col("home"), true))
          .divide(count("match"))
          .multiply(100).as("percent_play_home"),
        count(when(isInWorldCupUdf(col("competition")), true)).as("total_match_world_cup"),
        max("penalty_adversaire").as("max_penalty_adversaire"),
        sum("penalty_adversaire").minus(sum("penalty_france")).as("difference_penalty")
      )

    df_matches_stats.show()

    df_matches_stats.write.mode(SaveMode.Overwrite).parquet("src/main/resources/output/stats.parquet")

    /**
     * Partie 3 : jointure avec une autre source de données
     */
    df_matches_afterMarch1980
      .join(
        df_matches_stats,
        Seq("adversaire"),
        "left"
      )
      .withColumn("year", year(col("date")))
      .withColumn("month", month(col("date")))
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("year", "month")
      .parquet("src/main/resources/output/result.parquet")

    spark.sqlContext.read.parquet("src/main/resources/output/result.parquet").show()
  }

  def fillPenalty(point: String): Int = {
    point match {
      case "NA" => 0
      case null => 0
      case _ => point.toInt
    }
  }

  def isAtHome(_match: String): Boolean = {
    _match match {
      case null => false
      case _ => _match.toLowerCase().startsWith("france")
    }
  }

  def isInWorldCup(competition: String): Boolean = {
    competition match {
      case null => false
      case _ => competition.toLowerCase.startsWith("coupe du monde")
    }
  }

  def renameColumns(df: DataFrame, selectedColumns: Map[String, String]): DataFrame = {
    selectedColumns.foldLeft(df)((acc, item) => acc.withColumnRenamed(item._1, item._2))
  }

  def filterByDateGeq(df: DataFrame, selectedColumn: String, selectedDate: String): DataFrame = {
    df.filter(to_date(col(selectedColumn)).geq(lit(selectedDate)))
  }
}

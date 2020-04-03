package esgi.exo

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._


object FootballApp {

  val app_name = "footballapp"
  val app_master = "local[*]"
  val file_delimiter = ","
  val url_football_file = "src/main/resources/df_matches.csv"
  val save_mode = "overwrite"
  val url_parquet_stats_folder = "src/main/resources/parquet_football/stats.parquet"
  /***/
  val colname_match = "match"
  val colname_competition = "competition"
  val colname_year = "year"
  val colname_outcome = "outcome"
  val colname_date = "date"
  val colname_no = "no"
  val colname_penalty_france = "penalty_france"
  val colname_penalty_adversaire = "penalty_adversaire"
  val colname_played_home = "played_home"
  val colname_adversaire = "adversaire"
  val colname_score_france = "score_france"
  val colname_score_adversaire = "score_adversaire"
  val colname_pourcentage_match_domicile = "pourcentage_match_domicile"
  val colname_matchs_en_cdm = "matchs_en_cdm"
  var colname_ecart_penalty = "ecart_penalty"
  /***/
  val spark = SparkSession.builder()
    .appName(app_name)
    .master(app_master)
    .getOrCreate()
  import org.apache.spark.sql.functions.udf
  import spark.implicits._

  def main(args: Array[String]): Unit = {

    /** Nettoyer les donnees **/
    val dff = getFootballInfosDF()
      .withColumnRenamed("X4", colname_match)
      .withColumnRenamed("X6", colname_competition)
      .drop(colname_year)
      .drop(colname_outcome)
      .drop(colname_no)
      .na.fill(0)
      .filter(to_date(col(colname_date)).gt(lit("1980-03-01")))
    //dff.show

    /** Calculer les statistiques **/
    val df_stats = dff
      .withColumn(colname_played_home, newColumn(col(colname_match)))
        .groupBy(colname_adversaire)
        .agg(
          avg(colname_score_france),
          avg(colname_score_adversaire),
          count(colname_match), //nb matchs joué
          avg(when(col(colname_played_home),1))
            ./(count(colname_match))
            .as(colname_pourcentage_match_domicile),
          count(when(col(colname_competition).startsWith("Coupe du monde"),"true"))
            .as(colname_matchs_en_cdm),
          max(col(colname_penalty_adversaire)),
          count(col(colname_penalty_adversaire).-(col(colname_penalty_france)))
            .as(colname_ecart_penalty)
        )
    //df_stats.show
    df_stats.write
      .mode(save_mode)
      .parquet(url_parquet_stats_folder)

    /** Jointure avec une autre source de données **/
    val df_stats_parquet = 
  }

  /**
    * retourne une dataframe sur les infos football
    * @return DataFrame
    */
  def getFootballInfosDF() : DataFrame = {
    spark.read
      .option("delimiter", file_delimiter)
      .option("header", true)
      .csv(url_football_file)
  }


  val newColumn: UserDefinedFunction = udf(homeOrNot _)

  /**
    * permet de savoir si La France a joué à domicile ou non
    * @param s
    * @return Boolean
    */
  def homeOrNot(game : String) : Boolean = {
    if(game.startsWith("France")) {
      true
    } else {
      false
    }
  }
}

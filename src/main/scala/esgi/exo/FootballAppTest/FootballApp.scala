package esgi.exo.FootballAppTest

import java.time.LocalDate

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object FootballApp {
  val spark = SparkSession.builder().master("local[*]").appName("FootballApp").getOrCreate()
  val limitDate = LocalDate.parse("1980-03-01")
  val penaltyValueToIntUdf: UserDefinedFunction = udf(convertPenaltyToInt _)
  val isValidMatchValueUdf: UserDefinedFunction = udf(isValidMatchValue _)
  val isEqualOrPostMarch1980Udf: UserDefinedFunction = udf(isEquelOrPostMarch1980 _)
  val isHomeMatchUdf: UserDefinedFunction = udf(isHomeMatch _)

  def main(args: Array[String]): Unit = {

    val df_matches = buildDataFrame()
    val df_homeMatches = df_matches
        .withColumn("match_a_domicile", isHomeMatchUdf(col("match")))
    writeStatsParquet(df_homeMatches)
    writeResultParquet(df_homeMatches)
  }

  private def writeResultParquet(df_homeMatches: DataFrame) = {
    val stats = spark.sqlContext.read.parquet("src/main/resources/stats.parquet")

    df_homeMatches
      .join(stats, "adversaire")
      .withColumn("year", to_date(col("date"), "yyyy"))
      .withColumn("month", to_date(col("date"), "MM"))
      .write.mode(SaveMode.Overwrite)
      .partitionBy("year", "month")
      .parquet("src/main/resources/result.parquet")
  }

  private def writeStatsParquet(df: DataFrame) = {
    df.groupBy("adversaire")
      .agg(
        avg(col("score_france")).as("avg_france_score"),
        avg(col("score_adversaire")).as("avg_adversaire_score"),
        count(col("match")).as("count_match"),
        count(when(col("competition").contains("Coupe du monde"), 1)).as("count_match_coupe_du_monde"),
        count(when(col("match_a_domicile") ===  true, 1)).as("count_a_domicile"),
        max("penalty_france").as("max_penalty_france"),
        sum(col("penalty_france")).as("sum_penalty_france"),
        sum(col("penalty_adversaire")).as("sum_penalty_adversaire")
      )
      .withColumn("parcentage_match_a_domocile", col("count_a_domicile") / col("count_match") * 100)
      .withColumn("total_penalty_france_minus_total_penalty_adervsaire", col("sum_penalty_france") - col("sum_penalty_adversaire"))
      .write.mode(SaveMode.Overwrite)
      .parquet("src/main/resources/stats.parquet")
  }

  def buildDataFrame(): DataFrame = {
    spark.read.option("header", true)
      .option("delimiter", ",")
      .csv("src/main/resources/df_matches.csv")
      .withColumnRenamed("X4", "match") // A vs B
      .withColumnRenamed("X6", "competition")
      .filter(isValidMatchValueUdf(col("match")))
      .filter(col("competition") =!= "Qualifications pour la")
      .filter(col("date").geq("1980-03-01"))
      .withColumn("penalty_france", penaltyValueToIntUdf(col("penalty_france")))
      .withColumn("penalty_adversaire",penaltyValueToIntUdf(col("penalty_adversaire")))
      .select(col("match"),
        col("competition"),
        col("adversaire"),
        col("score_france"),
        col("score_adversaire"),
        col("penalty_france"),
        col("penalty_adversaire"),
        col("date"))
  }

  def convertPenaltyToInt(value: String): Int = value match {
    case null => 0
    case "NA" => 0
    case _ =>  value.toInt
  }

  def isHomeMatch(value: String): Boolean = {
    val team = value.split(" ")(0)
    team match {
      case "France" => true
      case _ => false
    }

  }

  def isValidMatchValue(value: String): Boolean = value match {
    case "\"France - Royaume des" => false
    case " Croates et Slovènes\"" => false
    case "Yougoslavie" => false
    case _ => true
  }

  def isEquelOrPostMarch1980(value:String): Boolean = LocalDate.parse(value) match {
    case  date => date.equals(date) || date.isAfter(date)
  }

}

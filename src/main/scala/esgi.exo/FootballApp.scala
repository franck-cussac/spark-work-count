package esgi.exo

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object FootballApp {

  val convertExactStringToIntegerUDF = udf(convertExactStringToInteger _)
  val matchIsDomicileUDF = udf(matchIsDomicile _)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate()

    val dfMatches = spark.read.option("delimiter", ",")
      .option("header", true)
      .csv("src/main/resources/df_matches.csv")

    val dfCleaned = cleanAndConvertData(dfMatches)

    startAndExportStats(dfCleaned)
    startAndExportJoined(spark, dfCleaned)
  }

  def startAndExportJoined(spark: SparkSession, dataFrame: DataFrame) = {
    val dfStats = spark.read.option("header", true).parquet("src/main/resources/stats.parquet")

    val dfJoined = dataFrame.join(dfStats, dataFrame("adversaire") === dfStats("adversaire"), "inner").drop(dfStats("adversaire"))
    val dfWithYearAndMonth = dfJoined.withColumn("year", year(col("date"))).withColumn("month", month(col("date")))

    dfWithYearAndMonth.write.partitionBy("year", "month").mode(SaveMode.Overwrite).parquet("src/main/resources/result.parquet")
  }

  def startAndExportStats(dataFrame: DataFrame) = {
    val dfDomicile = dataFrame.withColumn("domicile", matchIsDomicileUDF(col("match")))
    val dfGrouped = calculateStats(dfDomicile)

    dfGrouped.write.mode(SaveMode.Overwrite).parquet("src/main/resources/stats.parquet")
  }

  def cleanAndConvertData(dataFrame: DataFrame) : DataFrame = {
    val dfRenamed = dataFrame.withColumnRenamed("X4", "match")
      .withColumnRenamed("X6","competition")
      .select("match", "competition", "adversaire", "score_france", "score_adversaire", "penalty_france", "penalty_adversaire", "date")

    val dfConverted = dfRenamed.withColumn("penalty_france", convertExactStringToIntegerUDF(col("penalty_france")))
      .withColumn("penalty_adversaire", convertExactStringToIntegerUDF(col("penalty_adversaire")))
      .withColumn("date", to_date(col("date"), "yyyy-MM-dd"))

    dfConverted.filter(dfConverted("date").gt(lit("1980-03-01")))
  }

  def calculateStats(dataFrame: DataFrame) : DataFrame = {
    dataFrame.filter(!upper(col("adversaire")).rlike("FRANCE"))
      .groupBy("adversaire")
      .agg(
        avg("score_france").as("avg_score_france"),
        avg("score_adversaire").as("avg_score_adversaire"),
        count("match").as("total_match"),
        (count(when(col("domicile") === true, 1)) / count(col("match")) * 100).as("avg_total_match_domicile"),
        count(when(upper(col("competition")).contains("COUPE DU MONDE"), 1)).as("total_match_coupe_du_monde"),
        max("penalty_france").as("max_penalty_france"),
        (sum("penalty_france") - sum("penalty_adversaire")).as("ecart_penalty")
      ).select("adversaire", "avg_score_france", "avg_score_adversaire", "total_match", "avg_total_match_domicile",
      "total_match_coupe_du_monde", "max_penalty_france", "ecart_penalty")
  }

  def convertExactStringToInteger(value: String) : Int = {
    if (value.length > 0 && value.filter(Character.isDigit).length == value.length) {
      return value.toInt
    }

    return 0
  }

  def matchIsDomicile(value : String) : Boolean = {

    if (value.contains("-")) {
      val countryName = value.split('-')(0).trim()
      return countryName.toUpperCase() == "FRANCE"
    }

    return false
  }

}
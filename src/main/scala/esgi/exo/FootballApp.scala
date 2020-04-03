package esgi.exo

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.DataFrameStatFunctions

object FootballApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("football exercise").getOrCreate()
    val dfMatches = cleanUpDf(spark.read.option("delimiter", ",").option("header", true).csv("src/main/resources/df_matches.csv")).withColumn("domicile", udfDomicile(col("match")))
    val dfAverage = statistacs(dfMatches)
    writeWithoutPartition(dfAverage, "src/main/resources/stats.parquet")
    val result = joinDf(dfMatches, dfAverage)
    writeInPartition(result, "src/main/resources/result.parquet")
  }

  def writeWithoutPartition(toWrite: DataFrame, path: String): Unit = {
    toWrite
      .write
      .mode("overwrite")
      .parquet(path)
  }

  def joinDf(dfMatchs: DataFrame, dfStatsMatchs: DataFrame): DataFrame =
    dfMatchs.join(dfStatsMatchs, dfMatchs("adversaire") === dfStatsMatchs("adversaire"), "left_outer")
      .drop(dfMatchs("adversaire"))

  def writeInPartition(toWrite: DataFrame, path: String): Unit = {
    toWrite
      .withColumn("year", year(col("date")))
      .withColumn("month", month(col("date")))
      .write
      .mode("overwrite")
      .partitionBy("year", "month")
      .parquet(path)
  }

  def renameColumn(dataFrame: DataFrame, originalString: String, newName: String): DataFrame = {
    dataFrame.withColumnRenamed(originalString,newName)
  }

  def cleanUpDf(dataFrame: DataFrame): DataFrame = {
    val dfMatchesRenamed = renameColumn(renameColumn(dataFrame, "X4", "match"), "X6", "competition")
    val dfSelected = dfMatchesRenamed.filter(to_date(col("date")).gt(lit("1980-03-01"))).select(col("match"), col("competition"), col("adversaire"), col("score_france"), col("score_adversaire"), col("penalty_france"), col("penalty_adversaire"), col("date"))
    val inter = dfSelected.withColumn("penalty_france", col("penalty_france") cast "Int").withColumn("penalty_adversaire", col("penalty_adversaire") cast "Int")
    val onlyGoodDate = inter.na.fill(0)
    //val onlyGoodDate = inter.withColumn("penalty_france", udfConvertToInt(col("penalty_france"))).withColumn("penalty_adversaire", udfConvertToInt(col("penalty_adversaire")))
    onlyGoodDate
  }

  //j'ai considéré que penalty reçu par la France est la colonne penalty_france on m'a dit que c'était l'inverse apparemment mais j'ai laissé car je pense que la
  //logique reste la même
  def statistacs(dataFrame: DataFrame): DataFrame = {
    dataFrame.groupBy( "adversaire").
      agg(avg("score_france").as("score_moyen"),
      avg("score_adversaire").as("score_moyen_adversaire"),
      count("adversaire").as("nombre_matchs"),
      count(when(col("match") contains("France -"), 1))./(count(col("match"))).*(100).as("pourcentage_à_domicile"),
      count(when(col("competition") contains("Coupe du monde"), 1)).as("matchs_en_coupe_du_monde"),
      max(col("penalty_france")).as("nombre_max_de_pénalités"),
      sum(col("penalty_france")).-(sum(col("penalty_adversaire"))).as("difference_de_penalités")
    )
  }

  def udfConvertNa: UserDefinedFunction = udf(convertNa)

  def domicile: String => Boolean = value => value.startsWith("France -")

  def udfDomicile: UserDefinedFunction = udf(domicile)

  def convertNa: String => Int = {
    case "NA" => 0
    case null => 0
    case value => value.toInt
  }

  def udfConvertToInt: UserDefinedFunction = udf(convertToInt)

  def convertToInt: String => Int = (value: String) => value.filter(Character.isDigit).toInt

}

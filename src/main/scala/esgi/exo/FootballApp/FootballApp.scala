package esgi.exo.FootballApp

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{avg, _}

object FootballApp {
  def main(args: Array[String]): Unit = {
    val isDomicile: UserDefinedFunction = udf(isHome _)
    //val sumMatch: UserDefinedFunction = udf(nbMatch _)

    val spark = SparkSession.builder().appName("FootballApp").master("local[*]").getOrCreate()
    val df = spark.read.option("header", true).csv("src/main/resources/df_matches.csv")
    df.show(500)

    val dfMatchesClean = renameAndSelect(df)
    val dfMatches = deleteNullValue(dfMatchesClean)
    val dfMatchFrance = dfMatches.filter(col("date").between("1980-03-01",current_date())).withColumn("penalty_france",col("penalty_france").cast("integer")).withColumn("match_a_domicile",isDomicile(col("match")))
    dfMatchFrance.show


    val avgMatchFrance = avgScoreFrance(dfMatches)
    avgMatchFrance
      .show

    val avgMatchAdv = avgScoreAdversaire(dfMatches)
    avgMatchAdv.show

    val nbMa = sumMatchParEquipe(dfMatches)
    nbMa.show

    /*dfMatchesFrance
      .groupBy("adversaire")
      .agg(sum(when(col("match_a_domicile") === true, 1)) / count(col("match_a_domicile")) * 100).show*/

    val dfPercentageMatchHome= dfMatchFrance.groupBy("adversaire").agg(sum(when(col("match_a_domicile") === true,1)) / count(col("match_a_domicile")))

    dfPercentageMatchHome.show

    dfMatchFrance
      .groupBy("match")
      .max("penalty_france")
      .show(100)


  }

  def renameAndSelect(dataFrame: DataFrame) : DataFrame = {
    dataFrame
      .withColumnRenamed("X4","match")
      .withColumnRenamed("X6","competition")
      .withColumn("date",col("date").cast("date"))
      .select("match","competition","adversaire","score_france","score_adversaire","penalty_france","penalty_adversaire","date")
  }

  def deleteNullValue(dataFrame: DataFrame) : DataFrame = {
    dataFrame
      .withColumn("penalty_france",when(col("penalty_france")==="NA" || col("penalty_france").isNull ,"0"))
      .withColumn("penalty_adversaire",when(col("penalty_adversaire") ==="NA" || col("penalty_adversaire").isNull,"0"))
  }


  def isHome(colName: String): Boolean = {
    val eq = col(colName).toString().split('-')(0).trim
    eq match {
      case "France" => true
      case _ => false
    }
  }

  def avgScoreFrance(df: DataFrame): DataFrame = {
    df.groupBy("adversaire")
      .agg(avg(col("score_france").cast("integer")))
  }

  def avgScoreAdversaire (df: DataFrame): DataFrame = {
    df.groupBy("adversaire")
      .agg(avg(col("score_adversaire").cast("integer")))
  }

  def nbMatch(df:DataFrame): Integer = {
    df.collect().length
  }

  def sumMatchParEquipe(df:DataFrame): DataFrame = {
    df.groupBy("adversaire")
      .count()
      .as("Nombre de match")
  }



}

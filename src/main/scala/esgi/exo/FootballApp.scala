package esgi.exo


import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._


//Tout le code devra être écrit dans une classe et un
//package nommé
//
//esgi.exo.FootballApp (inspirez vous du précédent main pour écrire votre application)
//Tous les tests devront être écrit dans une casse et un
//package nommé
//
//esgi.exo.FootballAppTest (inspirez vous de la précédente classe de test pour écrire vos tests)
//On considère que cette application est différente de l ’ autre, vous devez créer un nouveau SparkSession.
//Vous devez utiliser le même SharedSparkSession que pour votre première application.
//Vous devez définir le master et l ’ appName pour que l ’ application puisse s ’ exécuter dans l ’ IDE.


object FootballApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate()

    // Get Matches
    val dfMatches = fetchMatches(spark)

    // Rename column X4
    val dfX2Renamed = renameColumn(dfMatches, "X4", "match")

    // Rename column X6
    val dfX6Renamed = renameColumn(dfX2Renamed, "X6", "competition")

    var dfClean = selectColumns(dfX6Renamed)

    val d = removeNull(dfClean: DataFrame)

    val dfMatchGt1980 = filterMatchGt1980(d)
    dfMatchGt1980.show(100, false)
  }

  def fetchMatches(sparkSession: SparkSession): DataFrame = {
    sparkSession.read.option("sep", ",").option("header", true).csv("src/main/resources/df_matches.csv")
  }

  def renameColumn(dataFrame: DataFrame, theOldName: String, theNewName: String): DataFrame = {
    dataFrame.withColumnRenamed(theOldName, theNewName)
  }


  def selectColumns(dataFrame: DataFrame): DataFrame = {
    dataFrame.select("match", "competition", "adversaire", "score_france", "score_adversaire", "penalty_france", "penalty_adversaire", "date")
  }

  def removeNull(dataFrame: DataFrame): DataFrame = {
    dataFrame.withColumn("penalty_france", replaceNullByZeroUDF(col("penalty_france")))
      .withColumn("penalty_adversaire", replaceNullByZeroUDF(col("penalty_adversaire")))
  }

  def filterMatchGt1980(dataFrame: DataFrame): DataFrame = {
    dataFrame.filter(dataFrame("date").gt(lit("1980-03-01")))
  }

  val replaceNullByZeroUDF: UserDefinedFunction = udf(replaceNullByZero _)

  def replaceNullByZero(value: String): String = {
       value match {
       case null => "0"
       case "NA" => "0"
       case _ => value
     }
  }

  def castDate(dataFrame: DataFrame): DataFrame = {
    dataFrame.withColumn("date",to_date(unix_timestamp(dataFrame.col("date"), "yyyy-MM-dd")
      .cast("timestamp")))
  }
}
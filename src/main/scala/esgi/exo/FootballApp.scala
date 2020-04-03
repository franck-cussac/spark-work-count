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

    val dfClean = selectColumns(dfX6Renamed)

    val df = clean_data(dfClean)

    val d = removeNull(df: DataFrame)

    val dfMatchGt1980 = filterMatchGt1980(d)

    val dfJoue = addColumn(dfMatchGt1980)

    dfJoue.show(20, false)
//

    val scoreFrance = getAvgScorceFrance(dfJoue)

    scoreFrance.show(800, false)
//    writeStats(scoreFrance)

    val ddd =  join(dfJoue, scoreFrance)

    ddd.show()

    writeResult(scoreFrance)
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
    dataFrame.withColumn("date", to_date(unix_timestamp(dataFrame.col("date"), "yyyy-MM-dd")
      .cast("timestamp")))
  }

  def addColumn(dataFrame: DataFrame): DataFrame = {
    dataFrame.withColumn("joue", splitColumn(col("match")))
  }

  val splitColumn: UserDefinedFunction = udf(mat _)


  def mat(thematch: String): Boolean = {
    val dat = thematch.trim.split(' ')(0)
    dat match {
      case "France" => true
      case _ => false
    }
  }

  def getAvgScorceFrance(dataFrame: DataFrame): DataFrame = {
    dataFrame.groupBy("adversaire").agg(
      avg(col("score_france")).as("avg_score"),
      avg(col("score_adversaire")).as("avg_score_adversaire"),
      count(col("match")).as("match_total"),
      (sum(when(col("joue") === true, 1)) / count(col("joue")) * 100).as("percentage_match_joue_domicile"),
      sum(when(col("competition").contains("Coupe du monde"), 1)).as("total_match_play_world_cup"),
      max(col("penalty_france")).as("max_number_penalty"),
      (count(col("penalty_france")).as("dss")  - count(col("penalty_adversaire"))).as("number_penalty_adversaire")
    )
  }



  val moyenneUdf: UserDefinedFunction = udf(moyenneB _)
  def moyenneB(thematch: String): Boolean = {
    val dat = thematch.trim.split(' ')(0)
    dat match {
      case "France" => true
      case _ => false
    }
  }

  def writeStats(dataFrame: DataFrame): Unit = {
    dataFrame.write
      .mode("overwrite")
      .parquet("src/main/resources/stats.parquet")
  }

  def writeResult(dataFrame: DataFrame): Unit = {
    dataFrame.write
      .mode("overwrite")
      .parquet("src/main/resources/result.parquet")
  }


  def clean_data(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .filter(clean(col("match") ))
  }

  val clean: UserDefinedFunction = udf(isValidLine _)

  def isValidLine(value: String): Boolean = {
    value match {
      case "Yougoslavie" => false
      case "\"France - Royaume des" => false
      case " Croates et Slovènes\"" => false
      case _ => true
    }
  }



  def join(dfPart1: DataFrame, dfPart2: DataFrame): DataFrame = {
    dfPart1.join(dfPart2, dfPart1("adversaire") === dfPart2("adversaire"), "left_outer")
      .drop("adversaire")
  }

  val cond: UserDefinedFunction = udf(isValidLine _)


}
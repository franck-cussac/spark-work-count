package esgi.exo

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object FootballApp {

  val spark = SparkSession.builder().getOrCreate()

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    val source =
      spark
        .read
        .option("sep",",")
        .option("header", "true")
      .csv("src/main/resources/df_matches.csv")

    val towrite = buildDFForOpponents(source)
   // towrite.write.mode("append").parquet("src/main/resources/foot1.parquet")
    towrite.show(10)

  }

  def rename(input: DataFrame, toChange: String, changeTo: String) :DataFrame= {
    input.withColumnRenamed(toChange,changeTo)
  }

  def filterColumns(input: DataFrame) :DataFrame= {
    input
      .select(
        col("match"),
        col("competition"),
        col("adversaire"),
        col("score_france"),
        col("score_adversaire"),
        col("penalty_france"),
        col("penalty_adversaire"),
        col("date")
      )
  }

  def filterByDate(input: DataFrame, filter: String): DataFrame = {
    input.
      where(col("date").gt(lit(filter)) || col("date") === (lit(filter))  )
  }

  def removeNullsToPenalties(input: DataFrame) : DataFrame = {
    input
      .withColumn("penalty_france", noDataTo0UDF(col("penalty_france")))
      .withColumn("penalty_adversaire", noDataTo0UDF(col("penalty_adversaire")))

  }
  val noDataTo0UDF = udf((x:String) => {noDataTo0(x)})
  def noDataTo0(input: String) : Int = input match {
    case input if input == "NA" => 0
    case input if input == "" => 0
    case input: String => try{input.toInt} catch { case e_ =>  0}
    case _ => 0
  }

  val isADomicileUDF = udf((x:String) => {isADomicile(x)})
  def isADomicile(input: String) : Boolean = input match {
    case input if  input.split('-')(0).trim() == "France" => true
    case _ => false
  }

  def matchesWithDomicileColumn(input:  DataFrame) : DataFrame = {
    input
      .withColumn("a_domicile", isADomicileUDF(col("match")))
  }

  def scoreFromOpponent(input : DataFrame):DataFrame = {
    input
      .withColumn("adversaire",col("adversaire"))
      .withColumn("score_adversaire",col("score_adversaire").cast("integer"))
      .groupBy(col("score_adversaire_adversaire")).avg("score_adversaire")
      .alias("score_from_opponent")
  }

  def scoreFromFrance(input: DataFrame):DataFrame = {
    input
      .withColumn("adversaire",col("adversaire"))
      .withColumn("score_france",col("score_france").cast("integer"))
      .groupBy(col("score_france_adversaire")).avg("score_france").alias("score_from_france")
  }

  def totalPlayedAgainsFrance(input: DataFrame) :DataFrame=
    input
      .withColumn("total_played_adversaire",col("adversaire"))
      .withColumn("score_france",col("score_france").cast("integer"))
      .groupBy(col("total_played_adversaire")).count().alias("total_played")

  def  worldCupEnconters(input: DataFrame):DataFrame =
    input
      .withColumn("competition", col("competition"))
      .withColumn("cdm_adversaire",col("adversaire"))
      .groupBy(col("cdm_adversaire"))
      .agg(count(when(col("competition").contains( "Coupe du monde"), true)).alias("Was world cup"))

  def maxPenaltiesFromFrance(input: DataFrame) :DataFrame =
    input
      .withColumn("penalty_france", col("penalty_france"))
      .withColumn("max_pen_fr_adversaire",col("adversaire"))
      .groupBy(col("max_pen_fr_adversaire"))
      .agg(max(col("penalty_france")).alias("max_pen"))

  def penlatiesDiff(input: DataFrame) : DataFrame=
    input
      .withColumn("penalty_france", col("penalty_france"))
      .withColumn("penalty_adversaire", col("penalty_adversaire"))
      .withColumn("diff_pen_adversaire",col("adversaire"))
      .groupBy(col("diff_pen_adversaire"))
      .agg(sum(col("penalty_france")).-(sum(col("penalty_adversaire"))).alias("pen_diff"))

  def buildDFForOpponents(input:  DataFrame) : DataFrame = {
    val a = filterByDate(input, "1980-03-01")
    val b = rename(a,"x4","match")
    val c = rename(b, "x6", "competition")
    val d = filterColumns(c)
    val e = removeNullsToPenalties(d)

    e.cache()

    val score_adversaire = scoreFromOpponent(e)
    val score_france = scoreFromFrance(e)
    val total_played = totalPlayedAgainsFrance(e)
    val cdm = worldCupEnconters(e)
    val max_pen_fr= maxPenaltiesFromFrance(e)
    val diff_pen = penlatiesDiff(e)

    val f =
      score_adversaire
        .join(score_france, score_adversaire("score_adversaire_adversaire")
          === score_france("score_france_adversaire"))
        .drop(col("score_france_adversaire"))
        .join(total_played, score_adversaire("score_adversaire_adversaire")
          === total_played("total_played_adversaire"))
        .drop(col("total_played_adversaire"))
        .join(cdm,col("cdm_adversaire")
          ===col("score_adversaire_adversaire"))
        .drop(col("cdm_adversaire"))
        .join(max_pen_fr, score_adversaire("score_adversaire_adversaire")
          === max_pen_fr("max_pen_fr_adversaire"))
        .drop(col("max_pen_fr_adversaire"))
        .join(diff_pen, score_adversaire("score_adversaire_adversaire")
          === diff_pen("diff_pen_adversaire"))
        .drop(col("diff_pen_adversaire"))
    f
  }






}

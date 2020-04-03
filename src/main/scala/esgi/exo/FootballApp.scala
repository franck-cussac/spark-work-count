package esgi.exo

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, _}

object FootballApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate()

    val df = spark.read.option("header", true).csv("src/main/resources/df_matches.csv")

    val atHomeUDF = spark.udf.register("atHomeUDF",atHome)

    // Rename et clean des colonnes
    val dfexo1 = exo1(df)
    // ajout de la colonne at_home
    val dfWithAtHome = dfexo1.withColumn("at_home", atHomeUDF(col("match")))
    // ajout des colonnes de stats
    val dfexo2 = exo2(dfWithAtHome)
    // ecriture dans un parquet
    writeParquet(dfexo2, "stats.parquet")
    // jointure parties 1 et 2
    val dfexo3 = exo3(dfexo1, dfexo2)
    // ecriture dans un parquet
    writeParquet(dfexo3, "result.parquet")
    dfexo3.show()

  }

  def exo1(dataFrame: DataFrame): DataFrame = {

    val df = selectAndRenameColumns(dataFrame)
    filterDate(df)
  }

  def exo2(dataFrame: DataFrame): DataFrame = {
    dataFrame.groupBy("adversaire").agg(
      avg("score_france").alias("average_goals_france"),
      avg("score_adversaire").alias("average_goals_opponent"),
      count("match").alias("number_of_matches"),
      count(when(col("at_home"), true)),
      count(when(col("competition").startsWith("Coupe du monde"), col("competition"))).alias("number_of_WC_matches"),
      sum(col("penalty_france")),
      sum(col("penalty_adversaire"))
    )
      .withColumn("home_percentage", col("count(CASE WHEN at_home THEN true END)") / col("number_of_matches") * 100)
      .withColumn("diff_penalty", col("sum(penalty_france)") - col("sum(penalty_adversaire)"))
      .drop("sum(penalty_adversaire)")
      .drop("sum(penalty_france)")
      .drop("count(CASE WHEN at_home THEN true END)")
  }

  def exo3(df1: DataFrame, df2: DataFrame): DataFrame = {
    df1.join(df2, df1("adversaire") === df2("adversaire")).drop("adversaire")
  }

  def selectAndRenameColumns(dataFrame: DataFrame): DataFrame = {

    val dfRenamedX4 = renameColumn(dataFrame, "X4", "match")

    renameColumn(dfRenamedX4, "X6", "competition")
      .select(
        "match",
        "competition",
        "adversaire",
        "score_france",
        "score_adversaire",
        "penalty_france" ,
        "penalty_adversaire",
        "date"
      )
      .withColumn("penalty_france", when(col("penalty_france") === "NA" || col("penalty_france") === "" , "0")
        .otherwise(col("penalty_france")))
      .withColumn("penalty_adversaire", when(col("penalty_adversaire") === "NA" || col("penalty_adversaire") === "" , "0")
        .otherwise(col("penalty_adversaire")))

  }

  def filterDate(dataFrame: DataFrame): DataFrame = {
    dataFrame.filter(col("year") >= "1980")
  }


  def atHome = (detail: String) => {
    if (detail.startsWith("France")) {
      true
    }
    else {
      false
    }
  }


  def renameColumn(dataFrame: DataFrame, oldC : String, newC : String): DataFrame = {
    dataFrame.withColumnRenamed(oldC, newC)
  }

  def writeParquet(dataFrame: DataFrame, name: String): Unit = {
    dataFrame.write.mode("overwrite").parquet("src/main/parquets/" + name)
  }
}

package esgi.exo

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object FootballApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate()
    val df1 = addColumnMonth(
      addColumnAtHome(
        replaceNullValues(
          selectColumns(
            renameColumns(
              keepOnlySince1980(
                getDF(spark)
              )
            )
          )
        )
      )
    )
    val dfStats = calculateStats(df1)
    writeParquetStats(dfStats)
    val join = joinDFBaseAndStats(df1, dfStats)
    writeParquetResult(join)
  }

  def getDF(spark: SparkSession): DataFrame = {
    spark.read.option("header", true).csv("src/main/resources/df_matches.csv")
  }

  def keepOnlySince1980(df: DataFrame): DataFrame = {
    df.filter(df("year") >= "1980")
  }

  def renameColumns(df: DataFrame): DataFrame = {
    df.withColumnRenamed("X4", "match")
      .withColumnRenamed("X6", "competition")
  }

  def selectColumns(df: DataFrame): DataFrame = {
    df.select(
      "match",
      "competition",
      "adversaire",
      "score_france",
      "score_adversaire",
      "penalty_france",
      "penalty_adversaire",
      "date",
      "year" // on en a besoin pour le partitionnement lors de l'enregistrement en parquet
    )
  }

  def replaceNullValues(df: DataFrame): DataFrame = {
    df.withColumn("penalty_france", extractInt(col("penalty_france")))
      .withColumn("penalty_adversaire", extractInt(col("penalty_adversaire")))
  }

  def addColumnAtHome(df: DataFrame): DataFrame = {
    df.withColumn("a_domicile", extractAtHome(col("match")))
  }

  // Nécessaire pour l'enregistrement en parquet
  def addColumnMonth(df: DataFrame): DataFrame = {
    df.withColumn("month", extractMonth(col("date")))
  }

  def calculateStats(df: DataFrame): DataFrame = {
    df.groupBy("adversaire")
      .agg(
        avg("score_france").as("avg_score_france"),                       // avg_score_france
        avg("score_adversaire").as("avg_score_adversaire"),               // avg_score_adversaire
        count("match").as("nb_matchs"),                                   // nb_matchs
        count(when(col("a_domicile"), true)).as("nb_a_domicile"),     // (nb_a_domicile, pour après)
        count(when(col("competition").startsWith("Coupe du monde"), col("competition")))
          .as("nb_matchs_CDM"),                                                        // nb matchs en coupe de monde
        max("penalty_france").as("max_penalty_france"),                   // max_penalty_france
        sum(col("penalty_france")).as("nb_penalty_france"),                  // (nb_penalty_france, pour après)
        sum(col("penalty_adversaire")).as("nb_penalty_adversaire")           // (nb_penalty_adversaire, pour après)
      )
      .withColumn("pourcentage_a_domicile", col("nb_a_domicile") * 100 / col("nb_matchs"))  // pourcentage_a_domicile
      .withColumn("indice_penalty", col("nb_penalty_france") - col("nb_penalty_adversaire"))// indice_penalty
      .drop("nb_a_domicile")         //
      .drop("nb_penalty_france")     // Suppression des colonnes utilitaires
      .drop("nb_penalty_adversaire") //
      .withColumnRenamed("adversaire", "adversaireBis") // colonne "adversaire" déjà présente dans le DF de base
  }

  def writeParquetStats(df: DataFrame): Unit = {
    df.write.mode("overwrite").parquet("src/main/parquets/stats.parquet")
  }

  def joinDFBaseAndStats(dfBase: DataFrame, dfStats: DataFrame): DataFrame = {
    dfBase.join(dfStats,
      dfBase("adversaire") === dfStats("adversaireBis"),
      "left_outer"
    )
      .drop("adversaireBis")
  }

  def writeParquetResult(df: DataFrame): Unit = {
    df
      .write
      .partitionBy("year", "month")
      .mode("overwrite")
      .parquet("src/main/parquets/result.parquet")
  }


  def replaceNABy0: String => String = s => if (s == "NA") "0" else s
  val extractInt: UserDefinedFunction = udf(replaceNABy0)

  def atHome: String => Boolean = s => s.startsWith("France")
  val extractAtHome: UserDefinedFunction = udf(atHome)

  def getMonthOfDate: String => String = s => s.substring(5, 7)
  val extractMonth: UserDefinedFunction = udf(getMonthOfDate)
}

package esgi.exo

import com.sun.org.apache.xpath.internal.functions.FuncSubstringAfter
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import scala.util.Try

object FootballApp {

  def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("football").getOrCreate()
    import spark.implicits._

    val df = spark.read.option("sep", ",").option("header", true).csv("src/main/resources/df_matches.csv")
    val output = "src/main/resources/output/stats.parquet"
    val output_joined = "src/main/resources/output/result.parquet"
    val cleaned = launcher(df, output)
    val stats = spark.sqlContext.read.parquet(output)
    joinSource(cleaned,stats, output_joined)
  }

  def launcher(df: DataFrame, output: String): DataFrame = {
    val dfSelected = firstClean(df)
    val dfDom = addDomicileFromMatch(dfSelected)
    val stats = adversairesStats(dfDom)
    writeToParquet(stats, output)
    return dfDom
  }

  def firstClean(df: DataFrame): DataFrame ={
    return df
      .where(length(col("date")) === 10)
      .withColumnRenamed("X4", "match")
      .withColumnRenamed("X6", "competition")
      .withColumn("penalty_france", when(col("penalty_france").equalTo("NA"), "0").otherwise(col("penalty_france")))
      .withColumn("penalty_adversaire", when(col("penalty_adversaire").equalTo("NA"), "0").otherwise(col("penalty_adversaire")))
      .where(col("date").substr(0, 7) >= "1980-03" )
      .select("match","competition","adversaire","score_france","score_adversaire","penalty_france","penalty_adversaire","date")
  }

  def adversairesStats(input: DataFrame): DataFrame = {
    return input.groupBy("adversaire").agg(
      avg(input("score_france")).alias("avg_france_pt"),
      avg(input("score_adversaire")).alias("avg_adv_pt"),
      count("match").alias("nb_match"),
      count(when(col("domicile") === true, 1)).as("percent_domicile"),
      count(when(col("competition").startsWith("Coupe du monde"), 1)).as("nb_coupe"),
      max(col("penalty_adversaire")).as("most_penalty"),
      (sum("penalty_adversaire") - sum("penalty_france")).as("diff_penalty")
    ).withColumn("percent_domicile", col("percent_domicile")*100/col("nb_match"))
  }


  def joinSource(left: DataFrame, right: DataFrame, output: String) = {
    val joinedDf = left.join(right, left.col("adversaire")===right.col("adversaire")).drop(right.col("adversaire"))
    val dfWithDate = joinedDf.withColumn("year", col("date").substr(0, 4)).withColumn("month", col("date").substr(6, 2))
    partitionParquet(dfWithDate, output)
  }

  def partitionParquet(df: DataFrame, output: String): Unit ={
    df.write.mode("overwrite").partitionBy("year", "month").parquet(output)
  }

  def writeToParquet(input: DataFrame, output: String) = {
    input.write.mode("overwrite").parquet(output)
  }

  def addDomicileFromMatch (input: DataFrame): DataFrame = {
    val domicile: UserDefinedFunction = udf(udfDomicile _)
    return input.withColumn("domicile",domicile(col("match")) )
  }

  def udfDomicile(input: String): Boolean = {
    input match {
      case x if x.startsWith("France") => return true
      case _ => return false
    }
  }

  val replace = udf((data: String , rep : String)=>data.replaceAll(rep, ""))
}

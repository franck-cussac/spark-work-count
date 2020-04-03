package esgi.exo

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object FootballApp {
  def main(args: Array[String]): Unit = {

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
    case input: String => input.toInt
    case _ => 0
  }
}

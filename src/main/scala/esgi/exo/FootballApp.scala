package esgi.exo

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, _}

object FootballApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate()

    val df = spark.read.option("header", true).csv("src/main/resources/df_matches.csv")

    val atHomeUDF = spark.udf.register("atHomeUDF",atHome)

    val dfexo1 = exo1(df)
    dfexo1.withColumn("at_home", atHomeUDF(col("match"))).show()


  }

  def exo1(dataFrame: DataFrame): DataFrame = {

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
      .filter(col("year") >= "1980")

  }

  //def exo2(dataFrame: DataFrame): DataFrame = {
  //dataFrame.withColumn("at_home", atHomeUDF(col("match"),"France"))
  //}


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

  def writeParquet(dataFrame: DataFrame): Unit = {
    dataFrame.write.mode("overwrite").parquet("src/main/parquets/output.parquet")
  }
}

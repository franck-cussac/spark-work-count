package esgi.exo

import java.util.{Calendar, Date}

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object FootballApp {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().appName("football").master("local[*]").getOrCreate()
        import spark.implicits._

        var matches = spark.read.option("delimiter", ",").option("header", true).csv("src/main/resources/df_matches.csv")

        matches = matches.withColumnRenamed("X4", "match")
        matches = matches.withColumnRenamed("X6", "competition")

        matches = matches.select("match", "competition", "adversaire", "score_france", "score_adversaire", "penalty_france", "penalty_adversaire", "date")

        val replaceNA = udf(_replaceNA _)
        matches = matches.withColumn("penalty_france", replaceNA($"penalty_france"))
        matches = matches.withColumn("penalty_adversaire", replaceNA($"penalty_adversaire"))

        matches = matches
            .filter(to_date(col("date")).geq(lit("1980-03-01")))

        val domicile = udf(_domicile _)
        matches = matches.withColumn("domicile", domicile($"match"))

        var stats = matches
            .groupBy("adversaire")
            .agg(
                avg("score_france").as("moyenne_score_france"),
                avg("score_adversaire").as("moyenne_score_adversaire"),
                count("*").as("nombre_match"),
                count(when($"domicile", true)).as("nombre_domicile")
            )

        stats.show()

        stats.write.parquet("stats.parquet")
    }

    def _domicile(m: String): Boolean = {
        m.split(" - ")(0) == "France"
    }

    def _replaceNA(penalty: String): String = {
        if (penalty.equals("NA")) {
            return "0"
        }

        penalty
    }
}

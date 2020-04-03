package xke.local

import esgi.exo.FootballApp
import org.scalatest.{FunSuite, GivenWhenThen}
import spark.{DataFrameAssertions, SharedSparkSession}

class FootballAppTest extends FunSuite with GivenWhenThen with DataFrameAssertions {
  val spark = SharedSparkSession.sparkSession
  import spark.implicits._

  test("je veux clean mon dataFrame") {
    Given("dataframe avec 13 colonnes : X2,X4,X5,X6,adversaire,score_france,score_adversaire,penalty_france,penalty_adversaire,date,year,outcome,no")
     val input = spark.sparkContext.parallelize(
      List(
        ("1er mai 1984", "Belgique - France", "3-3", "Match amical", "Belgique", "3", "3", "NA", "NA", "1984-05-01", "1984", "draw",1),
        ("12 février 1905", "France - Suisse", "1-0", "Match amical", "Suisse", "1", "0", "NA", "NA", "1905-02-12", "1905", "win",1),
        ("7 mai 1985", "Belgique - France", "7-0", "Match amical", "Belgique", "0", "7", "NA", "NA", "1985-05-07", "1985", "loss",1)
      )
    ).toDF("X2","X4","X5","X6","adversaire","score_france","score_adversaire","penalty_france","penalty_adversaire","date","year","outcome","no")

    When("rename X4 et X6, garder que les dates valides, mettre des 0 pour penalty NA, ngarder les matchs apres mars 1980")
    val actually = FootballApp.cleanDf(input)
    actually.show

    val expected = spark.sparkContext.parallelize(
      List(
        ("Belgique - France", "Match amical", "Belgique", "3", "3", "0", "0", "1984-05-01"),
        ("Belgique - France", "Match amical", "Belgique", "0", "7", "0", "0", "1985-05-07")
      )
    ).toDF("match", "competition", "adversaire", "score_france", "score_adversaire", "penalty_france", "penalty_adversaire", "date")

    assertDataFrameEquals(expected, actually)
  }

}

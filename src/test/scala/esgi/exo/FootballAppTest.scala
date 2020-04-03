package esgi.exo

import org.scalatest.{FunSuite, GivenWhenThen}
import spark.{DataFrameAssertions, SharedSparkSession}

class FootballAppTest extends FunSuite with GivenWhenThen with DataFrameAssertions {
  val spark = SharedSparkSession.sparkSession
  import spark.implicits._

  test("je veux renommer la colonnes X4 en match") {
    Given("une dataframe avec au moins 13 colonnes : X2, X4, X5, X6, adversaire, score_france, score_adversaire, penalty_france, penalty_adversaire, date, year, outcome, no")
    val input = spark.sparkContext.parallelize(
      List(
        ("1er mai 1904", "Belgique - France", "3-3", "Match amical", "Belgique", "3", "3", "NA", "NA", "1904-05-01", "1904", "draw", "1")
      ))
      .toDF(
        "X2", "X4", "X5", "X6", "adversaire", "score_france", "score_adversaire", "penalty_france", "penalty_adversaire", "date", "year", "outcome",  "no")

    val expected = spark.sparkContext.parallelize(
      List(
        ("1er mai 1904", "Belgique - France", "3-3", "Match amical", "Belgique", "3", "3", "NA", "NA", "1904-05-01", "1904", "draw", "1")
      ))
      .toDF(
        "X2", "match", "X5", "X6", "adversaire", "score_france", "score_adversaire", "penalty_france", "penalty_adversaire", "date", "year", "outcome",  "no")

    When("Je renomme")
    val actual = FootballApp.renameColumn(input,"X4", "match")

    Then("Je m'attend à ce que la colonne soit renommé")
    assertDataFrameEquals(actual, expected)
  }

  test("je veux selectionner certaines colonnes pour supprimer les colonnes  X2, X5, year, outcome") {
    Given("une dataframe avec au moins 13 colonnes : X2, match, X5, competition, adversaire, score_france, score_adversaire, penalty_france, penalty_adversaire, date, year, outcome, no")
    val input = spark.sparkContext.parallelize(
      List(
        ("1er mai 1904", "Belgique - France", "3-3", "Match amical", "Belgique", "3", "3", "NA", "NA", "1904-05-01", "1904", "draw", "1")
      ))
      .toDF(
        "X2", "match", "X5", "competition", "adversaire", "score_france", "score_adversaire", "penalty_france", "penalty_adversaire", "date", "year", "outcome",  "no")

    val expected = spark.sparkContext.parallelize(
      List(
        ("Belgique - France", "Match amical", "Belgique", "3", "3", "NA", "NA", "1904-05-01")
      ))
      .toDF(
        "match", "competition", "adversaire", "score_france", "score_adversaire", "penalty_france", "penalty_adversaire", "date")

    When("Je sélectionnee")
    val actual = FootballApp.selectColumns(input)

    Then("Je m'attend à ce que certaines colonnes soient supprimées")
    assertDataFrameEquals(actual, expected)
  }


}
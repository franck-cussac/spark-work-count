package esgi.exo.FootballAppTest

import org.apache.spark.sql.SparkSession
import org.scalatest.{FunSuite, GivenWhenThen}
import spark.{DataFrameAssertions, SharedSparkSession}
import esgi.exo.FootballApp.FranceFootballApp

class FranceFootballAppTest extends FunSuite with GivenWhenThen with DataFrameAssertions {

  val spark: SparkSession = SharedSparkSession.sparkSession
  import spark.implicits._

  /////------Part 1------////

  test("I would like to rename columns of my df 'X4' and 'X6' with 'match' and 'competition'" ){
    Given("df with columns 'X4' 'X6'")
    val input = List(
      ("1er mai 1904", "Belgique - France", "3-3", "Match amical", "Belgique"),
      ("1er mai 1904", "Belgique - France", "3-3", "Match amical", "Belgique"),
      ("1er mai 1904", "Belgique - France", "3-3", "Match amical", "Belgique")
    ).toDF("X2", "X4", "X5", "X6", "adversaire")

    val expected = List(
      ("1er mai 1904", "Belgique - France", "3-3", "Match amical", "Belgique"),
      ("1er mai 1904", "Belgique - France", "3-3", "Match amical", "Belgique"),
      ("1er mai 1904", "Belgique - France", "3-3", "Match amical", "Belgique")
    ).toDF("X2", "match", "X5", "competition", "adversaire")

    When("I apply my function 'renameColumn' of France Football app")
    val result = FranceFootballApp.renameDfColumns(input)

    Then("My columns with name 'X4' and 'X6' should be renamed with 'match' and 'competition'")
    result.columns.toSet shouldEqual expected.columns.toSet
  }

  test("I would like to replace 'NA' value with 0"){
    Given("List of string values with 'NA'")
    val input =  List("NA", "10", "NA", "460")
    val expected =  List(0, 10, 0, 460)

    When("I apply my function 'replaceNAWithZero'")
    val result = input.map(FranceFootballApp.replaceNAWithZero)
    Then("My function should replace all 'NA' value with 0")

    result shouldEqual(expected)
  }

  /////------Part 2------////
  test("I would like to filter strings list and keep only those begin with 'France'"){
    Given("List of Strings")
    val input = List("Belgique - France", "France - Belgique", "France - Belgique", "Pays-Bas - France", "France - Hongrie")
    val expected = List("France - Belgique", "France - Belgique", "France - Hongrie")

    When("I filter using my function 'isHomeMatch'")
    val result = input.filter(FranceFootballApp.isHomeMatch)

    Then("I should have only strings begin with 'France'")
    result shouldEqual(expected)
  }

  test("I would like to to calculate some stats"){
    Given("Mon dataframe, et ce que je veux en sortie")
    val input = List(
      ("France - Belgique" , "Coupe du monde 2006" , "Belgique" , "2", "1", "0", "0", true),
      ("France - Belgique" , "match amical"        , "Belgique" , "3", "2", "0", "1", true),
      ("Belgique - France" , "match amical"        , "Belgique" , "1", "0", "0", "0", false),
      ("Allemagne - France", "Coupe du monde 1998" , "Allemagne", "2", "1", "2", "1", false),
      ("Allemagne - France", "match amical"        , "Allemagne", "2", "1", "0", "0", false),
      ("France - Allemagne", "Coupe du monde 20012", "Allemagne", "1", "2", "2", "0", true),
      ("France - Allemagne", "match amical"        , "Allemagne", "3", "2", "0", "2", true),
      ("France - Colombie" , "match amical"        , "Colombie" , "3", "3", "0", "0", true),
      ("France - Colombie" , "match amical"        , "Colombie" , "3", "1", "0", "0", true)
    ).toDF(
      "match",
      "competition",
      "adversaire",
      "score_france",
      "score_adversaire",
      "penalty_france",
      "penalty_adversaire",
      "match_a_domicile"
    )

    val expected = List(
      ("Belgique", "2.0", "1.0", "3", "1", "66.66666666666667", "1", "1.0"),
      ("Allemagne", "2.0", "1.5", "4", "2", "50.0", "2", "-1.0"),
      ("Colombie", "3.0", "2.0", "2", "0", "100.0", "0", "0.0")
    ).toDF(
      "adversaire",
      "avg_score_france",
      "avg_score_adversaire",
      "total_nb_match",
      "total_nb_coupe_de_monde",
      "pourcentage_a_domicile",
      "max_penalty_reçu_france",
      "difference_pinalty"
    )

    When("I apply my function 'calculateStats'")
    val result = FranceFootballApp.calculateStats(input)

    Then("I should obtain my stats")
    assertDataFrameEquals(result, expected);
  }

  test("I would like to make join between two data frame") {
    Given("Thow data frames")
    val inputDf1 = List(
      ("France - Belgique", "Belgique", "2", "1", "0", "0", true),
      ("France - Belgique", "Belgique", "4", "5", "1", "0", true),
      ("France - Allemagne", "Allemagne", "3", "0", "0", "1", true),
      ("France - Allemagne", "Allemagne", "3", "2", "0", "2", true)
    ).toDF(
      "match",
      "adversaire",
      "score_france",
      "score_adversaire",
      "penalty_france",
      "penalty_adversaire",
      "a_domicile"
    )
    val inputDfStats = List(
      ("Belgique", "3.0", "3.0", "2", "100.0", "1", "1.0"),
      ("Allemagne", "6.0", "1.0", "2", "100.0", "0", "-3.0")
    ).toDF(
      "adversaire_bis",
      "avg_score_france",
      "avg_score_adversaire",
      "nb_matchs",
      "pourcentage_a_domicile",
      "max_penalty_france",
      "indice_penalty"
    )
    val expected = List(
      ("France - Belgique", "Belgique", "2", "1", "0", "0", true, "3.0", "3.0", "2", "100.0", "1", "1.0"),
      ("France - Belgique", "Belgique", "4", "5", "1", "0", true, "3.0", "3.0", "2", "100.0", "1", "1.0"),
      ("France - Allemagne", "Allemagne", "3", "0", "0", "1", true, "6.0", "1.0", "2", "100.0", "0", "-3.0"),
      ("France - Allemagne", "Allemagne", "3", "2", "0", "2", true, "6.0", "1.0", "2", "100.0", "0", "-3.0")
    ).toDF(
      "match",
      "adversaire",
      "score_france",
      "score_adversaire",
      "penalty_france",
      "penalty_adversaire",
      "a_domicile",
      "avg_score_france",
      "avg_score_adversaire",
      "nb_matchs",
      "pourcentage_a_domicile",
      "max_penalty_france",
      "indice_penalty"
    )

    When("I apply my join")
    val actual = FranceFootballApp.joinDFResultPart1AndDfStats(inputDf1, inputDfStats)

    Then("The join should be applied")
    assertDataFrameEquals(actual, expected)
  }

}
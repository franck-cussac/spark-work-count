package esgi.exo.FootballAppTest

import esgi.exo.FootballApp
import org.scalatest.{FunSuite, GivenWhenThen}
import spark.{DataFrameAssertions, SharedSparkSession}

class FootballAppTest extends FunSuite with GivenWhenThen with DataFrameAssertions {
  val spark = SharedSparkSession.sparkSession
  import spark.implicits._

  test("je veux recuperer mon mois d'une annee") {
    Given("une date sous format AAA-MM-JJ")
    val input = "1980-03-26"
    val expected = "03"

    When("j'extrait")
    val actual = FootballApp.extractMonthFromDate(input)

    Then("j'ai bien mon mois")
    assert(actual === expected)
  }

  test("l'equipe de france joue a domicile") {
    Given("un match de l'equipe de france")
    val input = "France - Suisse"
    val expected = true

    When("je regarde le match")
    val actual = FootballApp.homeOrNot(input)

    Then("elle joue bien a domicile")
    assert(actual === expected)
  }

  test("l'equipe de france joue a l'exterieur") {
    Given("un match de l'equipe de france")
    val input = "Suisse - France"
    val expected = false

    When("je regarde le match")
    val actual = FootballApp.homeOrNot(input)

    Then("elle joue bien a l'exterieur")
    assert(actual === expected)
  }

  test("je veux recuperer ma dataframe des matchs de l'equipe de france") {
    val columns = Array(
      "adversaire",
      "score_france",
      "score_adversaire"
    ).toSet

    When("J'appel ma methode")
    val actual = FootballApp.getFootballInfosDF().schema.fieldNames.toSet

    Then("J'ai mon dataframe")
    columns shouldEqual actual
  }
}

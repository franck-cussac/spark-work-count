package esgi.exo

import org.apache.spark.sql.SaveMode
import org.scalatest.{FunSuite, GivenWhenThen}
import spark.{DataFrameAssertions, SharedSparkSession}

class FootballAppTest extends FunSuite with GivenWhenThen with DataFrameAssertions {
  val spark = SharedSparkSession.sparkSession
  import spark.implicits._

  // unit tests
  test("Je veux renommer X1 en A et X2 en B") {
    Given("Un dataframe avec 3 colonnes : X1, X2 et X3")
    val inputDf = List(
      (1, 2, 3),
      (3, 4, 5),
      (5, 6, 7)
    ).toDF("X1", "X2", "X3")

    When("J'appelle renameColumns")
    val actualDf = FootballApp.renameColumns(
      inputDf,
      Map(
        "X1" -> "A",
        "X2" -> "B"
      )
    )

    Then("Un dataframe avec 3 colonnes : A, B, X3")
    val expectedDf = List(
      (1, 2, 3),
      (3, 4, 5),
      (5, 6, 7)
    ).toDF("A", "B", "X3")

    assertDataFrameEquals(actualDf, expectedDf)
  }

  test("Je veux mettre à 0 si NA / null ou renvoyer l'entier") {
    Given("Une liste de chaîne de caractères")
    val inputList = List("NA", null, "1")

    When("J'appelle fillPenalty")
    val actualList = inputList.map(FootballApp.fillPenalty)

    Then("Une liste d'entier")
    val expectedList = List(0, 0, 1)

    actualList should contain theSameElementsAs expectedList
  }

  test("Je veux filtrer les éléments avec une date supérieure à 1980-03-01") {
    Given("Un dataframe avec 2 colonnes : adversaire et date")
    val inputDf = List(
      ("Belgique", "1904-05-01"),
      ("Suisse", "1980-03-01"),
      ("Danemark", "1980-03-26"),
      ("Pérou", "2018-06-26")
    ).toDF("adversaire", "date")

    When("J'appelle filterByDateGeq")
    val actualDf = FootballApp.filterByDateGeq(inputDf, "date", "1980-03-01")

    Then("Un dataset avec 2 colonnes : adversaire et date")
    val expectedDf = List(
      ("Suisse", "1980-03-01"),
      ("Danemark", "1980-03-26"),
      ("Pérou", "2018-06-26")
    ).toDF("adversaire", "date")

    assertDataFrameEquals(actualDf, expectedDf)
  }

  test("Je veux vérifier que si un match commence par 'France', alors il est à domicile") {
    Given("Une liste de chaîne de caractères")
    val inputList = List(null, "France - Belgique", "france - belgique", "Belgique - France", "Belgique - Suisse")

    When("J'appelle isAtHome")
    val actualList = inputList.map(FootballApp.isAtHome)

    Then("Une liste de boolean")
    val expectedList = List(false, true, true, false, false)

    actualList should contain theSameElementsAs expectedList
  }

  test("Je veux vérifier que si une compétition commence par 'Coupe du monde', alors c'est une coupe de monde") {
    Given("Une liste de chaîne de caractères")
    val inputList = List(null, "Match amical", "Qualifications pour la Coupe du monde", "Coupe du monde", "coupe du monde")

    When("J'appelle isAtHome")
    val actualList = inputList.map(FootballApp.isInWorldCup)

    Then("Une liste de boolean")
    val expectedList = List(false, false, false, true, true)

    actualList should contain theSameElementsAs expectedList
  }

  // integration tests
}

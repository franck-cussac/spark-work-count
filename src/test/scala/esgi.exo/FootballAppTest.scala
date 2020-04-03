package esgi.exo

import org.scalatest.{FunSuite, GivenWhenThen}
import spark.{DataFrameAssertions, SharedSparkSession}

class FootballAppTest extends FunSuite with GivenWhenThen with DataFrameAssertions {
  val spark = SharedSparkSession.sparkSession

  import spark.implicits._

  test("je veux que mon match soit à domicile") {

    Given("un string avec le nom de la France et un autre pays séparé par un tiret")
    val input = "France - Suisse"

    val expected = true

    When("je demande si le match est à domicile")
    val actual = FootballApp.matchIsDomicile(input)

    Then("je dois avoir la valeur true")
    actual shouldEqual expected
  }

  test("je veux convertir ma chaine de caractere en nombre") {

    Given("un string avec uniquement des chiffres dedans")
    val input = "150"

    val expected = 150

    When("je convertis mon string en int")
    val actual = FootballApp.convertExactStringToInteger(input)

    Then("je dois avoir la valeur 150 en chiffre")
    actual shouldEqual expected
  }

  test("je veux vérifier que je nettoie et convertis mes données correctement") {

    Given("un dataframe avec les colonnes : X2,X4,X6,adversaire,score_france,score_adversaire,penalty_france,penalty_adversaire,date,year,outcome")
    val input = spark.sparkContext.parallelize(
      List(
        ("1er mai", "Belgique - France", "Match amical", "Belgique", "1", "0", "NA", "1", "1930-03-02", "1930", "win"),
        ("20 juin", "France - Angleterre", "Match amical", "Angleterre", "0", "1", "NA", "NA", "1984-06-27", "1984", "loss"),
        ("15 mars", "Italie - France", "Coupe du monde 1930", "Italie", "0", "0", "NA", "NA", "1985-11-16", "1985", "draw")
      )
    ).toDF("X2","X4","X6","adversaire","score_france","score_adversaire","penalty_france","penalty_adversaire","date","year","outcome")

    val expected = spark.sparkContext.parallelize(
      List(
        ("France - Angleterre", "Match amical", "Angleterre", 0, 1, 0, 0, "1984-06-27"),
        ("Italie - France", "Coupe du monde 1930", "Italie", 0, 0, 0, 0, "1985-11-16")
      )
    ).toDF("match","competition","adversaire","score_france","score_adversaire","penalty_france","penalty_adversaire","date")


    When("je nettoie et convertis mon dataframe")
    val actual = FootballApp.cleanAndConvertData(input)

    Then("je dois avoir un dataframe convertis et reduit")
    assertDataFrameEquals(actual, expected)
  }
}

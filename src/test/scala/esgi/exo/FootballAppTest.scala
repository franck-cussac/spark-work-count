package esgi.exo

import org.scalatest.{FunSuite, GivenWhenThen}
import spark.{DataFrameAssertions, SharedSparkSession}

class FootballAppTest extends FunSuite with GivenWhenThen with DataFrameAssertions {
  val spark = SharedSparkSession.sparkSession
  import spark.implicits._

  test("Je veux faire des corrections sur des chaînes de caractères : 'NA' => 0 / '4' => 4") {
    Given("Des chaînes de caractères à corriger : NA / null / 5")
    val input1 = "NA"
    val input2 = "null"
    val input3 = "5"

    When("Lorsque j'applique la correction")
    val actual1 = FootballApp.correctionPenalty(input1)
    val actual2 = FootballApp.correctionPenalty(input2)
    val actual3 = FootballApp.correctionPenalty(input3)

    Then("Les chaînes de caractères doivent être remplacées par des 0 et le résultat doit être un int")
    val expected1 = 0
    val expected2 = 0
    val expected3 = 5

    assert(actual1 === expected1)
    assert(actual2 === expected2)
    assert(actual3 === expected3)
  }

  test("Je veux savoir si le match joué est à domicile ou pas") {
    Given("Des libellés de matchs joués")
    val input1 = "France - Angleterre"
    val input2 = "Angleterre - France"

    When("J'applique ma fonction qui permet de déterminer si le match est à domicile ou non")
    val actual1 = FootballApp.atHome(input1)
    val actual2 = FootballApp.atHome(input2)

    Then("La valeur doit m'indiquer si le match est joué à domicile ou non")
    val expected1 = true
    val expected2 = false

    assert(actual1 === expected1)
    assert(actual2 === expected2)
  }

  test("Je veux savoir si c'est un match de coupe du monde") {
    Given("")
    val input1 = "Coupe du monde"
    val input2 = "Qualification à la coupe du monde"

    When("J'applique ma fonction qui détermine si c'est un match de coupe du monde")
    val actual1 = FootballApp.isWorldCup(input1)
    val actual2 = FootballApp.isWorldCup(input2)

    Then("La valeur doit m'indiquer si le match est un match de coupe du monde ou non")
    val expected1 = true
    val expected2 = false

    assert(actual1 === expected1)
    assert(actual2 === expected2)
  }

  test("Je veux faire la jointure entre les matchs et les stats") {
    Given("")
    val inputMatchs = List(
      ("Angleterre", 2, "toto"),
      ("Italie", 3, "toto"),
      ("Espagne", 4, "toto"),
      ("Croatie", 15, "tata"),
    ).toDF("adversaire", "code_departement", "nom_region")

    val inputStats = List(
      (2, "Nantes"),
      (15, "Reims"),
      (16, "Charleville"),
      (17, "Sedan")
    ).toDF("department_code", "name")

    val expected = List(

    )

    When("")
    val actual = FootballApp.joinDf(inputMatchs, inputStats)

    Then("")
    assertDataFrameEquals(actual, expected)
  }
}

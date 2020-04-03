package xke.local

import org.apache.spark.sql.catalyst.expressions.AssertTrue
import org.scalatest.{FunSuite, GivenWhenThen}
import spark.{DataFrameAssertions, SharedSparkSession}

class HelloWorldTest extends FunSuite with GivenWhenThen with DataFrameAssertions {
  val spark = SharedSparkSession.sparkSession
  import spark.implicits._


  test("je veux ajouter une colonne avec la moyenne des numéros département par région") {
    Given("J'ai un DF")
    val input= List(
      (1,"20","Reg1"),
      (1,"10","Reg1"),
      (1,"36","Reg1"),
      (4,"974","974LeMeilleurBled")
    ).toDF("code_region","code_departement","nom_region")

    val expected = List(
      (1,"Reg1",22.0),
      (4,"974LeMeilleurBled",974.0)
    ).toDF("code_region","nom_region", "avg(code_departement)")
    When("je lance limplementatione calcule de la moyenne")
    val actual = HelloWorld.avgDepByReg(input)

    Then("Je dois avoir le bon format de sortie")
    assertDataFrameEquals(expected,actual);
  }

 test("je veux renommer la colonne des moyennes des numéros département") {

    Given("une dataframe avec au moins 3 colonnes : nom région, code région et numéro département")
   val expected = List(
     (1,"Reg1",22.0),
     (4,"974LeMeilleurBled",974.0)
   ).toDF("code_region","nom_region", "avg_dep")
   val input = List(
     (1,"Reg1",22.0),
     (4,"974LeMeilleurBled",974.0)
   ).toDF("code_region","nom_region", "avg(code_departement)")
    When("J'utilse la fonction de rennomage")
      val actual = HelloWorld.renameColumn(input)
      actual.show()
    Then("")
    assertDataFrameEquals(actual, expected)
  }

  test("Quand je reçoit un nombre qui commence par 0, je renvoie Neal") {
    Given("J'ai un nombre commançant par 0")
    val input = "02"
    val expected = None;
    When("Je le filtre")
    val actual = HelloWorld.stringToInt(input)

    Then("Il a disparu")
    assert(expected == actual)

  }

  test("Quand je reçoit un nombre , je renvoie Neal") {
    Given("J'ai un nombre")
    val input = "2"
    val expected = Some(2);
    When("Je le filtre")
    val actual = HelloWorld.stringToInt(input)

    Then("Il change de format")
    assert(expected == actual)

  }

  test("Quand je reçoit un nombre qui contient une lettre, je renvoie Neal") {
    Given("J'ai un nombre avec un lettre")
    val input = "2B"
    val expected = None;
    When("Je le filtre")
    val actual = HelloWorld.stringToInt(input)

    Then("Il n'est plus là")
    assert(expected == actual)

  }
  test("Mes 3 datasets doivent être fusionner") {
    Given("J'ai 3 DataFrames")
    val villes =
      List(
        ("974","97480","Saint-Joseph")
      ).toDF(
        "department_code","zip_code","name"
      )

    val departements =
      List(
        ("04","974","La Réunion")
      ).toDF(
        "region_code","code","name"
      )

    val regions =
      List(
        ("04","La Réunion")
      ).toDF(
        "code","name"
      )
    val expected =
      List(
        (
        "974"
        ,"97480"
        ,"Saint-Joseph"
        ,"04"
        ,"La Réunion"
        ,"La Réunion")
      ).toDF(
        "department_code"
        ,"zip_code"
        ,"city_name"
        ,"region_code"
        ,"departement_name"
        ,"region_name"
      )

    When("Je les joins")
    val actual = HelloWorld.joinLocations(villes,departements,regions)

    Then("J'ai un seul DF complet")

    assertDataFrameEquals(actual,expected)

  }

  test("Full") {
    Given("J'ai un nombre avec un lettre")

    When("Je le filtre")

    Then("Il n'est plus là")
 //   HelloWorld.ExoJoin()

    assert("true"==="true")
  }
}

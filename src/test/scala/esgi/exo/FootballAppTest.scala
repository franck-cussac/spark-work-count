package esgi.exo

import org.scalatest.{FunSuite, GivenWhenThen}
import spark.{DataFrameAssertions, SharedSparkSession}


class FootballAppTest extends FunSuite with GivenWhenThen with DataFrameAssertions {
  val spark = SharedSparkSession.sparkSession
  import spark.implicits._

  test("Je veux écrire dans un dataFrame sans partition") {
    Given("Un dataframe avec 3 colonnes")
    val path = "src/main/resources/stats.parquet"
    val input = spark.sparkContext.parallelize(List(
      (1, 2, "toto"),
      (1, 3, "toto"),
      (1, 4, "toto")
    )).toDF("coco", "moto", "poco")

    When("j'écris dans un parquet")
    FootballApp.writeWithoutPartition(input, path)
    val expected = spark.read.parquet(path)

    Then("le dataframe lu dans le parquet doit être le même que celui écrit")
    assertDataFrameEquals(input, expected)
  }

  test("Je veux écrire dans un dataFrame avec partition") {
    Given("Un dataframe avec 3 colonnes")
    val path = "src/main/resources/stats.parquet"
    val input = spark.sparkContext.parallelize(List(
      (1, 2, "1981-11-18"),
      (1, 3, "1981-11-18"),
      (1, 4, "1981-11-18")
    )).toDF("coco", "moto", "date")

    When("j'écris dans un parquet")
    FootballApp.writeInPartition(input, path)
    val expected = spark.read.parquet(path)
    val compare = spark.sparkContext.parallelize(List(
      (1, 2, "1981-11-18", "1981", "11"),
      (1, 3, "1981-11-18", "1981", "11"),
      (1, 4, "1981-11-18", "1981", "11")
    )).toDF("coco", "moto", "date", "year", "month")
    Then("le dataframe lu dans le parquet doit être le même que celui écrit")
    assertDataFrameEquals(compare, expected)
  }

  test("Je veux renommer une colonne donnée") {
    Given("Un dataframe avec 3 colonnes : code_departement, code_region, nom_region")
    val input = spark.sparkContext.parallelize(List((1, 2, "toto"),
      (1, 2, "toto")
    )).toDF("code_region", "code_departement","nom_region")

    When("Je lance le renommage")
    val actual = FootballApp.renameColumn(input, "code_departement", "departements")

    Then("La colonne doit bien être renommée")
    val expected = spark.sparkContext.parallelize(List(
      (1, 2, "toto")
    )).toDF("code_region", "departements","nom_region")

    assertDataFrameEquals(actual, expected)
  }

  test("Je veux tester si le booléen est bien retourné à domicile") {
    Given("Un match non valide")
    val notValid = "Pologne - Canada"

    When("Je vérifie si la France joue à domicile")
    val result = FootballApp.domicile(notValid)

    Then("Le resultat doit être faux")
    assert(result === false)
  }

  test("je veux tester que la conversion de string en int fonctionne"){
    Given("une chaîne à convertir")
    val input = "075"
    When("Je convertis la chaîne d'entrée")
    val expected = 75
    Then("je m'attends à trouver 75")
    assert(FootballApp.convertToInt(input) === expected)
  }

}

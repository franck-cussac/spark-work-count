package xke.local

import org.scalatest.{FunSuite, GivenWhenThen}
import spark.{DataFrameAssertions, SharedSparkSession}

class HelloWorldTest extends FunSuite with GivenWhenThen with DataFrameAssertions {
  val spark = SharedSparkSession.sparkSession
  import spark.implicits._

  test("je veux ajouter une colonne avec la moyenne des numéros département par région") {
    Given("une dataframe avec au moins 3 colonnes : nom région, code departement et nom region")
    val input = spark.sparkContext.parallelize(List(
      (1, "toto", 2),
      (1, "toto", 3),
      (1, "toto", 4),
      (2, "zaza", 14)
    )).toDF("code_region", "nom_region", "code_departement")
    val expected = spark.sparkContext.parallelize(List(
      (1, "toto", 3),
      (2, "zaza", 14)
    )).toDF("code_region", "nom_region", "avg(code_departement)")

    When("on calcule la moyenne des numéros de départements par région")
    val actual = HelloWorld.avgDepByReg(input)

    Then("Je dois retrouver les moyennes par régions des départements")
    assertDataFrameEquals(actual, expected)
  }

  test("je veux renommer la colonne des moyennes des numéros département") {
    Given("une dataframe avec au moins 3 colonnes : nom région, code région et numéro département")
    val input = spark.sparkContext.parallelize(List(
      (1, "toto", 2),
      (2, "zaza", 14)
    )).toDF("code_region", "nom_region", "avg(code_departement)")
    val expected = spark.sparkContext.parallelize(List(
      (1, "toto", 2),
      (2, "zaza", 14)
    )).toDF("code_region", "nom_region", "avg_dep")

    When("Je renomme")
    val actual = HelloWorld.renameColumn(input, "avg(code_departement)", "avg_dep")

    Then("J'ai renommé")
    assertDataFrameEquals(actual, expected)
  }

  test("je veux vérifier que je lis un fichier, ajoute une colonne, la renomme, et sauvegarde mon fichier en parquet") {
    Given("une dataframe avec au moins 3 colonnes : nom région, code région et numéro département")
    val columns = Array(
      "code_region",
      "nom_region",
      "avg_dep"
    ).toSet

    When("Je lance le programme")
    HelloWorld.main(null)
    val actual = spark.read.parquet("src/main/resources/output.parquet").schema.fieldNames.toSet

    Then("J'ai mon paquet")
    columns shouldEqual actual

  }

  test("je veux Convertir '2' en Int") {
    Given("une dataframe avec au moins 3 colonnes : nom région, code région et numéro département")
    val input = "2"
    val expected = 2

    When("je convertit")
    val actual = HelloWorld.convertInt(input)

    Then("j'ai bien mon Int")
    actual shouldEqual expected
  }

  test("je veux Convertir '2A' en Int") {
    Given("une dataframe avec au moins 3 colonnes : nom région, code région et numéro département")
    val input = "2A"
    val expected = 2

    When("je convertit")
    val actual = HelloWorld.convertInt(input)

    Then("j'ai bien mon Int")
    assert(actual === expected)
  }

  test("je veux Convertir '02' en Int") {
    Given("une dataframe avec au moins 3 colonnes : nom région, code région et numéro département")
    val input = "02"
    val expected = 2

    When("je convertit")
    val actual = HelloWorld.convertInt(input)

    Then("j'ai bien mon Int")
    assert(actual === expected)
  }

}

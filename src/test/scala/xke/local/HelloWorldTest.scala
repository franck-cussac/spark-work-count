package xke.local

import org.scalatest.{FunSuite, GivenWhenThen}
import spark.{DataFrameAssertions, SharedSparkSession}

class HelloWorldTest extends FunSuite with GivenWhenThen with DataFrameAssertions {
  val spark = SharedSparkSession.sparkSession
  import spark.implicits._

  test("je veux ajouter une colonne avec la moyenne des numéros département par région") {
    Given("Les données d'entrées et ce que je souhaite en sortie")
    val input = List(
      (1, 2, "toto"),
      (1, 3, "toto"),
      (1, 4, "toto"),
      (2, 14, "zaza"),
      (2, 54, "zaza"),
      (2, 7, "zaza"),
      (2, 5, "zaza")
    ).toDF("code_region", "code_departement", "nom_region")
    val expected = List(
      (1, 3.0, "toto"),
      (2, 20.0, "zaza")
    ).toDF("code_region", "avg(code_departement)", "nom_region")

    When("on calcule la moyenne des numéros de départements par région")
    val actual = HelloWorld.avgDepByReg(input)

    Then("Je dois retrouver les moyennes par régions des départements")
    assertDataFrameEquals(actual, expected)
  }

  test("je veux renommer la colonne des moyennes des numéros département") {
    val input = List(
      (1, 3.0, "toto"),
      (2, 20.0, "zaza")
    ).toDF("code_region", "avg(code_departement)", "nom_region")

    val expected = List(
      (1, 3.0, "toto"),
      (2, 20.0, "zaza")
    ).toDF("code_region", "avg_dep", "nom_region")

    When("on renomme la colonne des moyennes")
    val actual = HelloWorld.renameColumn(input)

    Then("Je avoir la colonne renommée")
    assertDataFrameEquals(actual, expected)
}

  test("je veux vérifier que je lis un fichier, ajoute une colonne, la renomme, et sauvegarde mon fichier en parquet") {
    Given("Ce que je souhaite en sortie")
    val expected = List(
      (1, 3.0, "toto"),
      (2, 20.0, "zaza")
    ).toDF("code_region", "avg_dep", "nom_region")

    When("on calcule les moyennes, on renomme la colonne des moyennes, on écrit, puis on lit")
    HelloWorld.main(Array[String]("test"))
    val actual = spark.read.parquet("src/main/parquets/output.parquet")

    Then("On devrait obtenir le DF après le renommage")
    assertDataFrameEquals(actual, expected)
  }

  test("je veux convertir '033' en int") {
    Given("une string '033', et ce que je souhaite : 33")
    val input = "033"
    val expected = 33

    When("Je convertit en int")
    val actual = HelloWorld.stringToInt(input)

    Then("je devrais obtenir 33")
    assert(actual === expected)
  }

  test("je veux convertir 'A33' en int") {
    Given("une string 'A33', et ce que je souhaite : 33")
    val input = "A33"
    val expected = 33

    When("Je convertit en int")
    val actual = HelloWorld.stringToInt(input)

    Then("je devrais obtenir 33")
    assert(actual === expected)
  }

  test("je veux convertir '33' en int") {
    Given("une string '33', et ce que je souhaite : 33")
    val input = "33"
    val expected = 33

    When("Je convertit en int")
    val actual = HelloWorld.stringToInt(input)

    Then("je devrais obtenir 33")
    assert(actual === expected)
  }

  test("je veux faire la jointure entre les départements et les villes") {
    Given("Les départements et les villes, et ce que je souhaite en sortie")
    val inputDepts = List(
      (1, 2, "toto"),
      (1, 3, "toto"),
      (1, 4, "toto"),
      (2, 14, "zaza"),
      (2, 54, "zaza"),
      (2, 7, "zaza"),
      (2, 5, "zaza")
    ).toDF("code_region", "code_departement", "nom_region")
    val inputCities = List(
      (2, "Paris"),
      (3, "Lille"),
      (4, "Lyon"),
      (5, "Bordeaux")
    ).toDF("department_code", "name")
    val expected = List(
      (2, "Paris", 1, "toto"),
      (3, "Lille", 1, "toto"),
      (4, "Lyon", 1, "toto"),
      (5, "Bordeaux", 2, "zaza"),
      (14, null, 2, "zaza"),
      (54, null, 2, "zaza"),
      (7, null, 2, "zaza")
    ).toDF("code_departement", "name", "code_region", "nom_region")

    When("Je fais la jointure")
    val actual = HelloWorld.joinDeptsAndCities(inputDepts, inputCities)

    Then("La jointure devrait être correcte")
    assertDataFrameEquals(actual, expected)
  }

  //1) utiliser List().toDF() pour créer vos dataframe d'input
  //2) assurez vous que toutes vos fonctions ont des tests
  //3) terminez bien votre main en ajoutant l'UDF développé ce matin
  //4) pensez à pull la branche master, j'ai corrigé la création du jar
  //5) pour ceux qui peuvent en local, réessayez de lancer un spark-submit avec --master spark://spark-master:7077 depuis le conteneur worker
  //Pour les autres, on verra peut être cet après-midi
}

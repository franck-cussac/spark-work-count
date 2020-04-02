package xke.local

import org.scalatest.{FunSuite, GivenWhenThen}
import spark.{DataFrameAssertions, SharedSparkSession}


class HelloWorldTest extends FunSuite with GivenWhenThen with DataFrameAssertions {
  val spark = SharedSparkSession.sparkSession
  import spark.implicits._

  test("Je veux ajouter une colonne avec la moyenne des numéros de département par région") {
    Given("Un dataframe avec 3 colonnes : code_departement, code_region, nom_region")
    val input = spark.sparkContext.parallelize(List(
      (1, 2, "toto"),
      (1, 3, "toto"),
      (1, 4, "toto")
    )).toDF("code_region", "code_departement","nom_region")

    When("Je lance la fonction")
    val actual = HelloWorld.avgDepByReg(input)

    Then("les moyennes doivent être calculées")
    val expected = spark.sparkContext.parallelize(List(
      (1, "toto", 3.0)
    )).toDF("code_region", "nom_region", "avg(code_departement)")

    assertDataFrameEquals(actual, expected)
  }

  test("Je veux renommer une colonne donnée") {
    Given("Un dataframe avec 3 colonnes : code_departement, code_region, nom_region")
    val input = spark.sparkContext.parallelize(List((1, 2, "toto"),
      (1, 2, "toto")
    )).toDF("code_region", "code_departement","nom_region")

    When("Je lance le renommage")
    val actual = HelloWorld.renameColumn(input, "code_departement", "departements")

    Then("La colonne doit bien être renommée")
    val expected = spark.sparkContext.parallelize(List(
      (1, 2, "toto")
    )).toDF("code_region", "departements","nom_region")

    assertDataFrameEquals(actual, expected)
  }


  test("Je veux tester que la lecture d'un fichier, le calcul de moyenne de département selon la région, le renommage d'une colonne et l'écriture du résultat dans un parquet fonctionnent"){
    Given("le lancement de la méthode main")
    HelloWorld.departementExercises()
    When("je vérifie les différentes colonnes du parquet")
    val df = spark.read.parquet("src/main/resources/toto")
    Then("les colonnes doivent être les même que dans le jeu de données de résultat")
    assert(df("avg_dep") != null && df("nom_region") != null && df("code_region") != null)
  }

  test("je veux tester que la conversion de string en int fonctionne"){
    Given("une chaîne à convertir")
    val input = "075"
    When("Je convertis la chaîne d'entrée")
    val expected = 75
    Then("je m'attends à trouver 75")
    assert(HelloWorld.convertToInt(input) === expected)
  }

  test("je veux joindre deux dataFrame sur le champs code_departement") {
    Given("deux listes présentant le champs code_departement")
    val inputDepts = spark.sparkContext.parallelize(List(
      (1, 2, "Ile de France"),
      (1, 3, "Ile de France"),
      (1, 4, "Ile de France"),
      (34, 6, "Haut de France"),
      (34, 7, "Haut de France"),
      (34, 5, "Haut de France"),
      (34, 8, "Haut de France")
    )).toDF("code_region", "code_departement", "nom_region")
    val inputCities = spark.sparkContext.parallelize(List(
      (2, "Paris"),
      (3, "Strasbourg"),
      (4, "Nantes"),
      (5, "Toulouse")
    )).toDF("department_code", "name")
    val expected = spark.sparkContext.parallelize(List(
      (2, "Paris", 1, "Ile de France"),
      (3, "Strasbourg", 1, "Ile de France"),
      (8, null, 34, "Haut de France"),
      (4, "Nantes", 1, "Ile de France"),
      (5, "Toulouse", 34, "Haut de France"),
      (6, null, 34, "Haut de France"),
      (7, null, 34, "Haut de France")
    )).toDF("code_departement", "name", "code_region", "nom_region")

    When("Je fais la jointure")
    val actual = HelloWorld.doJoinOnDepartCode(inputDepts, inputCities)

    Then("La jointure devrait être correcte")
    assertDataFrameEquals(actual, expected)
  }

  test("Je veux tester que la jointure de deux DF et leur écriture dans un parquet fonctionne"){
    Given("le lancement de la méthode main")
    HelloWorld.jointureDepartVille()
    When("je vérifie les différentes colonnes du parquet")
    val df = spark.read.parquet("src/main/resources/joinPartition")
    Then("les colonnes doivent exister")
    assert(df("code_region") != null && df("code_departement") != null)
  }

}

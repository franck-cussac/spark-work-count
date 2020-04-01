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

/*  test("main must create a file with word count result") {
    Given("input filepath and output filepath")
    val input = "src/test/resources/input.txt"
    val output = "src/test/resources/output/v1/parquet"

    When("I call word count")
    HelloWorld.main(Array(input, output))
    val expected = spark.sparkContext.parallelize(
      List(("rapidement",1),
        ("te",1),
        ("à",1),
        ("mots",1),
        ("des",1),
        ("s'il",1),
        ("compter",1),
        ("Bonjour,",1),
        ("as",1),
        ("plait.",1),
        ("tu",1))
    ).toDF("word", "count")

    Then("I can read output file and find my values")
    val actually = spark.sqlContext.read.parquet(output)

    assertDataFrameEquals(actually, expected)
  }
*/
  test("Je veux tester que la lecture d'un fichier, le calcul de moyenne de département selon la région, le renommage d'une colonne et l'écriture du résultat dans un parquet fonctionnent"){
    Given("le lancement de la méthode main")
    HelloWorld.main(null)
    When("je vérifie les différentes colonnes du parquet")
    val df = spark.read.parquet("src/main/resources/toto")
    Then("les colonnes doivent être les même que dans le jeu de données de résultat")
    assert(df("avg_dep") != null && df("nom_region") != null && df("code_region") != null)
  }

}
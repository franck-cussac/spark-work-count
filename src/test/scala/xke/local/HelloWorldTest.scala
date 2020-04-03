package xke.local

import org.scalatest.{FunSuite, GivenWhenThen}
import spark.{DataFrameAssertions, SharedSparkSession}

class HelloWorldTest extends FunSuite with GivenWhenThen with DataFrameAssertions {
  val spark = SharedSparkSession.sparkSession
  import spark.implicits._

  test("je veux ajouter une colonne avec la moyenne des numéros département par région") {
    Given("une dataframe avec au moins 3 colonnes : code_region, code-departement, et nom_region ")
    val input = spark.sparkContext.parallelize(
      List(
        (1, 64, "Pyrénées-Atlantiques"),
        (1, 24, "Dordogne"),
        (1, 95, "Val-d'Oise"),
        (2, 75, "Ile-de-france"),
        (2, 56, "Morbihan"),
        (2, 46, "Lot")

      )).toDF( "code_region", "code_departement", "nom_region")

    val expected = spark.sparkContext.parallelize(
      List(
        (1, 61, "Orne"),
        (2, 59, "Nord")
      )).toDF("code_region", "avg(code_departement)", "nom_region")

    When("J'applique ma fonction avgDepByReg je dois avoir une colonne avec la moyenne des numéros département par région")
    val actual =   HelloWorld.avgDepByReg(input)
    assertDataFrameEquals(actual, expected);
  }

  test("je veux renommer la colonne des moyennes des numéros département") {
    val input = spark.sparkContext.parallelize(List(
      (1, 3.0, "toto"),
      (2, 20.0, "zaza")
    )).toDF("code_region", "avg(code_departement)", "nom_region")

    val expected = spark.sparkContext.parallelize(List(
      (1, 3.0, "toto"),
      (2, 20.0, "zaza")
    )).toDF("code_region", "avg_dep", "nom_region")

    When("on renomme la colonne des moyennes")
    val actual = HelloWorld.renameColumn(input)

    Then("Je avoir la colonne renommée")
    assertDataFrameEquals(actual, expected)
  }

  test("je veux vérifier que je lis un fichier, ajouter une colonne, la renommer, et sauvegarder mon fichier en parquet") {
    Given("a dataframe from file")
    spark.read.option("sep", ",").option("header", true).csv("src/main/resources/departements-france.csv")

    val expected = spark.sparkContext.parallelize(
      List(
        (84 , 1, "Vaucluse"),
        (32 , 2, "Gers")
      )
    ).toDF("code_region", "avg_dep", "nom_region")

    When("I call main")
    HelloWorld.main(null)
    val main = spark.read.parquet("src/main/parquet/ex1.parquet")

    Then("result")
    assertDataFrameEquals(main, expected)
  }

  test("Je récupère que les chiffres") {

    Given("J'ai une chaine de caractère avec 2 chiffre: 1 et 5")
    val s = "1jgfmkk5"
    val expected = 15

    When("Je supprime les lettres")
    val actual = HelloWorld.parseInteger(s)

    Then("Le nombre de colonne devrait être 15")
    assert(actual === expected)
  }

  test("Test de la jointure") {

    Given("Je possède deux dataFrames")
    val input1 = spark.sparkContext
      .parallelize(List(
        (1, 25, "toto"),
        (2, 42, "tata"),
        (1, 14, "tutu"),
        (2, 36, "titi")
    )).toDF( "code_region", "code_departement","nom_region")


    val input2 = spark.sparkContext
      .parallelize(List(
        (14, "lolo"),
        (42, "lala"),
        (25, "lulu"),
        (36, "lili")
    )).toDF("department_code","nom_ville")

    When("Jointure sur les 2 dataframes'")
    val actual = HelloWorld.joinDf(input1,input2)

    val expected = spark.sparkContext.parallelize(List(
      (1, 14, "tutu", 14, "lolo"),
      (1, 25, "toto", 92, "lulu"),
      (2, 42, "tata", 42, "lala"),
      (1, 14, "tutu", 75, "lolo")
    )).toDF("code_region", "code_departement", "nom_region", "department_code", "nom_ville")
    Then("Je devrais avoir 5 colonnes")

    assertDataFrameEquals(actual,expected)
  }

  test("Test de la création des partitions") {
    Given("Je crée ma partition")
    HelloWorld.main(null)

    When("Je compte le nombre de colonnes de mon parquet")
    val actual = spark.read.parquet("src/main/parquet/code_region=04/code_departement=974/")


    Then("Le nombre de colonne doit être 10")
    val expected = 10

    assert(actual.columns.length === expected)
  }

}
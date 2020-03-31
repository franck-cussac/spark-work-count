package xke.local

import org.scalatest.{FunSuite, GivenWhenThen}
import spark.{DataFrameAssertions, SharedSparkSession}

class HelloWorldTest extends FunSuite with GivenWhenThen with DataFrameAssertions {
  val spark = SharedSparkSession.sparkSession
  import spark.implicits._

  /*test("main must create a file with word count result") {
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
  }*/

  test("je veux ajouter une colonne avec la moyenne des numéros département par région") {

    Given("une dataframe avec au moins 3 colonnes : code région, numéro département et nom région")
    val input = spark.sparkContext.parallelize(
      List(
        (84, 2, "Auvergne-Rhône-Alpes"),
        (84, 3, "Auvergne-Rhône-Alpes"),
        (84, 4, "Auvergne-Rhône-Alpes"),
        (32, 14, "Hauts-de-France"),
        (32, 54, "Hauts-de-France"),
        (32, 7, "Hauts-de-France"),
        (32, 9, "Hauts-de-France")
      )
    ).toDF("code_region", "code_departement", "nom_region")

    val expected = spark.sparkContext.parallelize(
      List(
        (84, 3, "Auvergne-Rhône-Alpes"),
        (32, 21, "Hauts-de-France")
      )
    ).toDF("code_region", "avg(code_departement)", "nom_region")

    When("calcul de la moyenne")
    val actual = HelloWorld.avgDepByReg(input)

    Then("je dois avoir les moyennes par régions")
    assertDataFrameEquals(actual, expected)
  }

  test("je veux renommer la colonne des moyennes des numéros département") {

    Given("une dataframe avec au moins 3 colonnes : code région, avg(code_departement) et nom région")
    val input = spark.sparkContext.parallelize(
      List(
        (84, 3, "Auvergne-Rhône-Alpes"),
        (32, 21, "Hauts-de-France")
      )
    ).toDF("code_region", "avg(code_departement)", "nom_region")

    val expected = spark.sparkContext.parallelize(
      List(
        (84, 3, "Auvergne-Rhône-Alpes"),
        (32, 21, "Hauts-de-France")
      )
    ).toDF("code_region", "avg_dep", "nom_region")

    When("renommage de la colonne avg(code_departement) en avg_dep")
    val actual = HelloWorld.renameColumn(input)

    Then("je dois avoir la colonne renommée en avg_dep")
    assertDataFrameEquals(actual, expected)

  }

  test("je veux vérifier que je lis un fichier, ajoute une colonne, la renomme, et sauvegarde mon fichier en parquet") {

    Given("une dataframe avec au moins 4 colonnes et un entête : code_departement, nom_departement, code_region et nom_region")

    val df = spark.sparkContext.parallelize(
      List(
        (1, "Ain", 84, "Auvergne-Rhône-Alpes"),
        (2, "Aisne", 32, "Hauts-de-France"),
        (3, "Allier", 84, "Auvergne-Rhône-Alpes")
      )
    ).toDF("code_departement", "nom_departement", "code_region", "nom_region")

    val exportPath = "src/test/resources/myOutput.parquet"

    val expected = Set("code_region", "avg_dep", "nom_region")

    When("je lance le traitement de mon fichier et je récupère le parquet")
    HelloWorld.start(spark, df, exportPath)
    val actual = spark.read.parquet(exportPath).columns.toSet

    Then("je dois avoir les 3 colonnes : code_region, avg_dep, nom_region")
    actual shouldEqual expected
  }

}

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
    Given("list of departements")
    val input = spark.sparkContext.parallelize(
      List(
        ("La Réunion", 974, 4),
        ("Guyane", 973, 3)))
      .toDF("nom_region", "code_region", "code_departement")

    val expected = spark.sparkContext.parallelize(
      List(
        ( 974.0, 4,  "La Réunion"),
        ( 973.0 , 3, "Guyane")))
      .toDF("code_region", "avg(code_departement)", "nom_region")

    When("alcule de la moyenne avgDepByReg")
    val actual = HelloWorld.avgDepByReg(input)

    Then("Result Expected when is ok")
    assertDataFrameEquals(actual, expected)
  }

  test("je veux ajouter une colonne avec la moyenne des numéros département par région when is Ko") {
    Given("list of departements")
    val input = spark.sparkContext.parallelize(
      List(
        ("La Réunion", 974, 4),
        ("Guyane", 973, 3)))
      .toDF("nom_region", "code_region", "code_departement")

    val expected = spark.sparkContext.parallelize(
      List(( 974.0, 4,  "La Réunion"), ( 978.0 , 3, "Guded")))
      .toDF("code_region", "avg(code_departement)", "nom_region")

    When("Calcule de la moyenne avgDepByReg")
    val actual = HelloWorld.avgDepByReg(input)

    Then("Résultat non valid.")
    assert(actual !== expected)
  }


  test("je veux renommer la colonne des moyennes des numéros département Ok") {

    Given("une dataframe avec au moins 3 colonnes : nom région, code région et numéro département")
    val input = spark.sparkContext.parallelize(
      List(
        ("La Réunion", 974, 4),
        ("Guyane", 973, 3)))
      .toDF("nom_region", "code_region", "code_departement")

    val expected = spark.sparkContext.parallelize(
      List(
        ( 974.0, 4,  "La Réunion"),
        ( 973.0 , 3, "Guyane")))
      .toDF("code_region", "avg(code_departement)", "nom_region")

    When("Je renomme")
    val actual = HelloWorld.renameColumn(input,"avg(code_departement)", "avg_dep")

    Then("Result Expected when is ok")
    assertDataFrameEquals(actual, expected)
  }

  test("je veux renommer la colonne des moyennes des numéros département Ko") {

    Given("une dataframe avec au moins 3 colonnes : nom région, code région et numéro département")
    val input = spark.sparkContext.parallelize(
      List(
        ("La Réunion", 974, 4),
        ("Guyane", 973, 3)))
      .toDF("nom_region", "code_region", "code_departement")

    val expected = spark.sparkContext.parallelize(
      List(
        ( 974.0, 4,  "La Réunion"),
        ( 973.0 , 3, "Guyane")))
      .toDF("code_region", "avg(code_departement)", "nom_region")

    When("Call function rename")
    val actual = HelloWorld.renameColumn(input,"avg(code_departement2)", "avg_dep")

    Then("Résultat attendu ko")
    assert(actual !== expected)
  }

  test("je veux vérifier que j'ai écris dans mon fichier") {
    Given("une dataframe avec au moins 3 colonnes : nom région, code région et numéro département")
    val input = spark.sparkContext.parallelize(
      List(
        ("La Réunion", 974, 4),
        ("Guyane", 973, 3)))
      .toDF("nom_region", "code_region", "code_departement")

    When("Call function rename")
    HelloWorld.writeDepartementInParquet(input)

    Then("Read  File when is ok")
    spark.read.parquet("src/main/resources/file.parquet").columns.length shouldEqual  3
  }

  test("Je veux convertir '02fers7' en entier") {
    Given("Une chaîne de caractères '027'")
    val input = "02fers7"
    val expected = 27

    When("Je convertis en entier")
    val actual = HelloWorld.convertStringToInt(input)

    Then("J'aimerais avoir 27")
    actual shouldEqual expected
  }



  test("Je veux convertir 'efdefd65' en entier") {
    Given("Une chaîne de caractères '65'")
    val input = "efdefd65"
    val expected = 65

    When("Je convertis en entier")
    val actual = HelloWorld.convertStringToInt(input)

    Then("J'aimerais avoir 65")
    actual shouldEqual expected
  }

  test("Je veux convertir 'a01443h' en entier") {
    Given("Une chaîne de caractères '1443h'")
    val input = "a01443h"
    val expected = 1443

    When("Je convertis en entier")
    val actual = HelloWorld.convertStringToInt(input)

    Then("J'aimerais avoir 1443")
    actual shouldBe expected
  }

  test("je veux vérifier que j'ai écris dans mon fichier 2") {
    Given("une dataframe avec au moins 3 colonnes : nom région, code région et numéro département")
    val input = spark.sparkContext.parallelize(
      List(
        (13, "Gard", 50, "bifbfk"),
        (867, null, 49, "testj"),
        (200, "Paris", 19, "efefefe"),
        (250, "Saint-Pierre-et-Miquelon", 18, "fdfedfd"),
        (44, "Saint-Barthélemy", 98, "pesto"),
        (14, null, 80, "bibo")
      ))
      .toDF("code_departement", "name", "code_region", "nom_region")

    When("Je renomme")
    HelloWorld.writeParquetByRegionAndDept(input)

    Then("Je m'attends que la lecture du parquet ait le bon nombre de lignes")
    spark.read.parquet("src/main/resources/code_region-code_departement.parquet").count() shouldEqual  6
  }

  test("Je veux faire la jointure entre les départements et les villes") {
    Given("Une dataframe de départements et cities")
    val departements = List(
      (50, 13, "bifbfk"),
      (49, 867, "testj"),
      (19, 200, "efefefe"),
      (18, 250, "fdfedfd"),
      (98, 44, "pesto"),
      (80, 14, "bibo")
    ).toDF("code_region", "code_departement", "nom_region")

    val cities = List(
      (13, "Gard"),
      (200, "Paris"),
      (250, "Saint-Pierre-et-Miquelon"),
      (44, "Saint-Barthélemy")
    ).toDF("department_code", "name")

    val expected = List(
      (13, "Gard", 50, "bifbfk"),
      (867, null, 49, "testj"),
      (200, "Paris", 19, "efefefe"),
      (250, "Saint-Pierre-et-Miquelon", 18, "fdfedfd"),
      (44, "Saint-Barthélemy", 98, "pesto"),
      (14, null, 80, "bibo")
    ).toDF("code_departement", "name", "code_region", "nom_region")

    When("Je joins")
    val actual = HelloWorld.mergeDepartementsAndCities(departements, cities)

    Then("J'aimerais que les dataFrame soient bonnes")
    assertDataFrameEquals(actual, expected)
  }

}

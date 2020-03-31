package xke.local

import org.assertj.core.api.Assertions
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
      List(("La Réunion", 974, 4), ("Guyane", 973, 3)))
      .toDF("nom_region", "code_region", "code_departement")

    val expected = spark.sparkContext.parallelize(
      List(( 974.0, 4,  "La Réunion"), ( 973.0 , 3, "Guyane")))
      .toDF("code_region", "avg(code_departement)", "nom_region")

    When("Call function avgDepByReg")
    val actual = HelloWorld.avgDepByReg(input)

    Then("Result Expected when is ok")
    assertDataFrameEquals(actual, expected)
  }

  test("je veux ajouter une colonne avec la moyenne des numéros département par région when is Ko") {
    Given("list of departements")
    val input = spark.sparkContext.parallelize(
      List(("La Réunion", 974, 4), ("Guyane", 973, 3)))
      .toDF("nom_region", "code_region", "code_departement")

    val expected = spark.sparkContext.parallelize(
      List(( 974.0, 4,  "La Réunion"), ( 978.0 , 3, "Guded")))
      .toDF("code_region", "avg(code_departement)", "nom_region")

    When("Call function avgDepByReg")
    val actual = HelloWorld.avgDepByReg(input)

    Then("Result Expected when is ko")
    assert(actual !== expected)
  }


  test("je veux renommer la colonne des moyennes des numéros département Ok") {

    Given("une dataframe avec au moins 3 colonnes : nom région, code région et numéro département")
    val input = spark.sparkContext.parallelize(
      List(("La Réunion", 974, 4), ("Guyane", 973, 3)))
      .toDF("nom_region", "code_region", "code_departement")

    val expected = spark.sparkContext.parallelize(
      List(( 974.0, 4,  "La Réunion"), ( 973.0 , 3, "Guyane")))
      .toDF("code_region", "avg(code_departement)", "nom_region")

    When("Call function rename")
    val actual = HelloWorld.renameColumn(input,"avg(code_departement)", "avg_dep")

    Then("Result Expected when is ok")
    assertDataFrameEquals(actual, expected)
  }

  test("je veux renommer la colonne des moyennes des numéros département Ko") {

    Given("une dataframe avec au moins 3 colonnes : nom région, code région et numéro département")
    val input = spark.sparkContext.parallelize(
      List(("La Réunion", 974, 4), ("Guyane", 973, 3)))
      .toDF("nom_region", "code_region", "code_departement")

    val expected = spark.sparkContext.parallelize(
      List(( 974.0, 4,  "La Réunion"), ( 973.0 , 3, "Guyane")))
      .toDF("code_region", "avg(code_departement)", "nom_region")

    When("Call function rename")
    val actual = HelloWorld.renameColumn(input,"avg(code_departement2)", "avg_dep")

    Then("Result Expected when is ok")
    assert(actual !== expected)
  }

  test("je veux vérifier que j'ai écris dans mon fichier") {
    Given("une dataframe avec au moins 3 colonnes : nom région, code région et numéro département")
    val input = spark.sparkContext.parallelize(
      List(("La Réunion", 974, 4), ("Guyane", 973, 3)))
      .toDF("nom_region", "code_region", "code_departement")

    When("Call function rename")
    HelloWorld.write(input,"C:\\Users\\christopher\\Downloads\\sort\\file.parquet")

    Then("Read  File when is ok")
    assert(spark.read.parquet("C:\\Users\\christopher\\Downloads\\sort\\file.parquet").columns.length === 3)
  }

}

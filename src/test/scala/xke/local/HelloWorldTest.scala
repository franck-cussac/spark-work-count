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

}

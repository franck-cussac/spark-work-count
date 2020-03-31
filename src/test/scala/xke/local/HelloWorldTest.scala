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
    Given("dataframe avec 3 colonnes : nom région, code région, numé département")
    val input = spark.sparkContext.parallelize(
      List(
        ("Ile de france", 10 , 75),
        ("Ile de france", 10 , 75),
        ("Ile de france", 10 , 75),
        ("Aquitaine", 20 , 50)
      )
    ).toDF("nom_region", "code_region", "code_departement")


    val expected = spark.sparkContext.parallelize(
      List(
        (10 , 75, "Ile de france"),
        ( 20 , 50, "Aquitaine")
      )
    ).toDF("code_region", "avg_departement", "nom_region")

    When("calcule average")
    val actual = HelloWorld.avgDepByReg(input)

    Then("return ok")
    assertDataFrameEquals(actual, expected)
  }

  test("je veux renommer la colonne des moyennes des numéros département") {

    Given("dataframe avec 3 colonnes : nom région, code région, numé département")
    val input = spark.sparkContext.parallelize(
      List(
        (32 , 2, "Hauts-de-France"),
        ( 20 , 50, "Aquitaine")
      )
    ).toDF("code_region", "avg(code_departement)", "nom_region")


    val expected = spark.sparkContext.parallelize(
      List(
        (32 , 2, "Hauts-de-France"),
        ( 20 , 50, "Aquitaine")
      )
    ).toDF("code_region", "avg_dep", "nom_region")

    When("rename column")
    val actual = HelloWorld.renameColumn(input, "avg_dep", "avg(code_departement)" )

    Then("column renamed")
    assertDataFrameEquals(actual, expected)
  }

  test("je veux vérifier que je lis un fichier, ajoute une colonne, la renomme, et sauvegarde mon fichier en parquet") {

  }

}

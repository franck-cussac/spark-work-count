package xke.local

import org.scalatest.{FunSuite, GivenWhenThen}
import spark.{DataFrameAssertions, SharedSparkSession}

class HelloWorldTest extends FunSuite with GivenWhenThen with DataFrameAssertions {
  val spark = SharedSparkSession.sparkSession
  import spark.implicits._

  test("main must create a file with word count result") {
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

  test("Je veux ajouter une colonne avec la moyenne des numéros de département par région") {
    // do something
  }

  test("Je veux renommer la colonne des moyennes des numéros de département") {
    Given("Une dataframe avec 3 colonnes : nom région, code région et numéro de département")
    val input = spark.sparkContext.parallelize(List(
      ("Ile-de-France", 10, 75),
      ("Ile-de-France", 10, 75),
      ("Ile-de-France", 10, 75),
      ("Aquitaine", 5, 33)
    )).toDF("region", "code_region", "departement")

    // do something
  }

  test("Je veux vérifier que je lis un fichier, ajoute une colonne, la renomme et sauvegarde mon fichier en parquet") {
    // do something
  }
}

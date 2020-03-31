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
    Given("Un dataframe avec une région et deux département pour une moyenne")

    val input = spark.sparkContext.parallelize(
      List(
        ("12", "Ile de france", 75),
        ("10", "Ile de france", 75),
        ("8", "Ile de france", 75),
        ("5", "Auvergne",  70)
      )
    ).toDF("code_departement", "nom_region", "code_region")

    When("I call the avg function")

    val actual = HelloWorld.avgDepByReg(input)

    Then("Dataframe are same")

    val expected = spark.sparkContext.parallelize(
      List(
        (75, 10, "Ile de france"),
        (70, 5, "Auvergne")
      )
    ).toDF("code_region", "avg(code_departement)", "nom_region")

    assertDataFrameEquals(actual, expected)

  }

  test("je veux renommer la colonne des moyennes des numéros département") {

    Given("Un dataframe avec la colonne avg")
    val input = spark.sparkContext.parallelize(
      List(
        (75, 10, "Ile de france"),
        (70, 5, "Auvergne")
      )
    ).toDF("code_region", "avg(code_departement)", "nom_region")

    When("I call the rename function")
    val actual = HelloWorld.renameColumn(input)

    Then("")
    val expected = spark.sparkContext.parallelize(
      List(
        (75, 10, "Ile de france"),
        (70, 5, "Auvergne")
      )
    ).toDF("code_region", "avg_dep", "nom_region")

    assertDataFrameEquals(actual, expected)
  }

//  test("je veux vérifier que je lis un fichier, ajoute une colonne, la renomme, et sauvegarde mon fichier en parquet") {
//
//  }

}

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

  }

  test("je veux renommer la colonne des moyennes des numéros département") {

    Given("une dataframe avec au moins 3 colonnes : nom région, code région et numéro département")
    val input = spark.sparkContext.parallelize(
      List(
        ("IdF", 1, 10),
        ("IdF", 1, 20),
        ("IdF", 1, 30),
        ("Occitanie", 4, 31)
      )
    ).toDF("nom_region", "code_region", "code_departement")


    val expected = spark.sparkContext.parallelize(
      List(
      ("Idf", 1, 20.0),
      ("Occitanie", 4, 31.0)
    )
    ).toDF("nom_region", "code_region", "avg(code_departement)")

    When("On créait une colonne avec la moyenne des numéro département par code région et les colonnes code_region, avg_dep, nom_region")

    val actual = HelloWorld.avgDepByReg(input)

    Then("On devrait obtenir expected")
    assertDataFrameEquals(actual, expected)
  }

  test("je veux vérifier que je lis un fichier, ajoute une colonne, la renomme, et sauvegarde mon fichier en parquet") {

  }

}

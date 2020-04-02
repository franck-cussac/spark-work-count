package xke.local

import org.apache.spark.sql
import org.scalatest.{FunSuite, GivenWhenThen}
import spark.{DataFrameAssertions, SharedSparkSession}

import scala.reflect.io.Directory
import java.io.File
import java.util.Calendar

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
    Given("dataframe avec 3 colonnes : nom région, code région, numéro département")
    val input = spark.sparkContext.parallelize(
      List(
        ("Ile de france", 10 , 73),
        ("Ile de france", 10 , 75),
        ("Ile de france", 10 , 77),
        ("Aquitaine", 20 , 50)
      )
    ).toDF("nom_region", "code_region", "code_departement")


    val expected = spark.sparkContext.parallelize(
      List(
        (10 , 75, "Ile de france"),
        ( 20 , 50, "Aquitaine")
      )
    ).toDF("code_region", "avg(code_departement)", "nom_region")

    When("calcule average")
    val actual = HelloWorld.avgDepByReg(input)

    Then("return ok")
    assertDataFrameEquals(actual, expected)
  }

  test("je veux renommer la colonne des moyennes des numéros département") {

    Given("dataframe avec 3 colonnes : nom région, code région, moyenne des départements")
    val input = spark.sparkContext.parallelize(
      List(
        (10 , 75, "Ile de france"),
        ( 20 , 50, "Aquitaine")
      )
    ).toDF("code_region", "avg(code_departement)", "nom_region")


    val expected = spark.sparkContext.parallelize(
      List(
        (10 , 75, "Ile de france"),
        ( 20 , 50, "Aquitaine")
      )
    ).toDF("code_region", "avg_dep", "nom_region")

    When("rename column")
    val actual = HelloWorld.renameColumn(input)

    Then("column renamed")
    assertDataFrameEquals(actual, expected)
  }

  test("je veux vérifier que je lis un fichier, ajoute une colonne, la renomme, et sauvegarde mon fichier en parquet") {
    val input = "src/test/resources/departements-france.csv"


    val expected = spark.sparkContext.parallelize(
      List(
        (84 , 1, "Auvergne-Rhone-Alpes"),
        (32 , 2, "Hauts-de-France")
      )
    ).toDF("code_region", "avg_dep", "nom_region")

    val df = spark.read.option("sep", ",").option("header", true).csv(input)
    val output = "src/test/resources/output/test3.parquet"

    When("calcule average")
    HelloWorld.writeToParquet(HelloWorld.renameColumn(HelloWorld.avgDepByReg(df)), output)

    val actually = spark.sqlContext.read.parquet(output)

    assertDataFrameEquals(actually, expected)

  }

  /*test("join test") {
    val input = spark.sparkContext.parallelize(
      List(
        (10 , 75, "Ile de france"),
        ( 20 , 50, "Aquitaine")
      )
    ).toDF("code_region", "avg(code_departement)", "nom_region")


    val expected = spark.sparkContext.parallelize(
      List(
        (10 , 75, "Ile de france"),
        ( 30 , 50, "Aquitaine")
      )
    ).toDF("code_region", "avg_dep", "sauce")

    val df = input.join(expected, input("code_region") === expected("code_region"))
    df.show()
  }*/

  test("je veux vérifier que je les 0 soit supprimé"){
    val input = "01"

    val actually = HelloWorld.udfZero(input)

    val expected = 1

    assert(actually == expected)
  }

  test("je veux vérifier que le B soit supprimé"){
    val input = "2A"

    val actually = HelloWorld.udfZero(input)

    val expected = 2

    assert(actually == expected)
  }



}

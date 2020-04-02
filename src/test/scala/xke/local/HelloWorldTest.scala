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
    val actual = HelloWorld.renameColumn(input,"avg(code_departement)", "avg_dep")

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
    HelloWorld.firstCSV(df, output)

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

  test("je veux vérifier que la jointure est bonne") {

    val inputVille = spark.sparkContext.parallelize(
      List(
        (1 , 1000, "Suresnes", 1),
        (2 , 2000, "Puteaux", 1),
        (3 , 3000, "Nanterre", 2)
      )
    ).toDF("id", "habitant", "nom", "department_code")

    val inputDepartement = spark.sparkContext.parallelize(
      List(
        (1 , 1, "Hauts-de-Seine", 1),
        (2 , 2, "Val d'oise", 2)
      )
    ).toDF("id_dep", "department_code", "nom_departement", "region_code")

    val inputRegion = spark.sparkContext.parallelize(
      List(
        (1 , 1, "Ile de France"),
        (2 , 2, "Rhone Alpes")
      )
    ).toDF("id_reg", "region_code", "nom_region")

    val expected = spark.sparkContext.parallelize(
      List(
        (1 , 1000, "Suresnes", 1,1,  "Hauts-de-Seine", 1, 1 ,  "Ile de France"),
        (2 , 2000, "Puteaux", 1, 1, "Hauts-de-Seine", 1, 1 ,  "Ile de France"),
        (3 , 3000, "Nanterre", 2, 2, "Val d'oise", 2, 2 ,  "Rhone Alpes")
      )
    ).toDF("id", "habitant", "nom","department_code","id_dep" ,"nom_departement","region_code" , "id_reg","nom_region")

    When("joinin cities / departments / regions")

    val actually = HelloWorld.secondCSV(inputVille, inputDepartement, inputRegion)

    assertDataFrameEquals(actually, expected)
  }

  test("je veux vérifier que la partition du fichier soit bonne"){
    Given("dataframe avec 3 colonnes : nom région, code région, moyenne des départements")
    val input = spark.sparkContext.parallelize(
      List(
        (1 , 1000, "Suresnes", 1,1,  "Hauts-de-Seine", 1, 1 ,  "Ile de France"),
        (2 , 2000, "Puteaux", 1, 1, "Hauts-de-Seine", 1, 1 ,  "Ile de France"),
        (3 , 3000, "Nanterre", 2, 2, "Val d'oise", 2, 2 ,  "Rhone Alpes")
      )
    ).toDF("id", "habitant", "nom","department_code","id_dep" ,"nom_departement","region_code" , "id_reg","nom_region")

    val output = "src/test/resources/output/join/"
    val sub_output = "src/test/resources/output/join/region_code=1"

    When("partitionning city department and region")
    HelloWorld.partitionParquet(input, output)
    val actually = spark.sqlContext.read.parquet(sub_output)

    assert(actually.count() === 2 )
  }

}

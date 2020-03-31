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
    Given("J'ai un DF")
    val input= List(
      (1,"20","reg1"),
      (1,"10","Reg1"),
      (1,"36","Reg1"),
      (4,"974","974LeMeilleurBled")
    ).toDF("code_region","code_departement","nom_region")

    val expected = List(
      (1,"Reg1",22.0),
      (4,"974LeMeilleurBled",974.0)
    ).toDF("code_region","nom_region", "avg(code_departement)")
    When("je lance limplementatione calcule de la moyenne")
    val actual = HelloWorld.avgDepByReg(input)

    Then("Je dois avoir le bon format de sortie")
    assertDataFrameEquals(expected,actual);
  }

  test("je veux renommer la colonne des moyennes des numéros département") {

    Given("une dataframe avec au moins 3 colonnes : nom région, code région et numéro département")
    val input = ???
    val expected = ???

    When("")
    val actual = HelloWorld.avgDepByReg(input)

    Then("")
   // assertDataFrameEquals(actual, expected)
  }

  test("je veux vérifier que je lis un fichier, ajoute une colonne, la renomme, et sauvegarde mon fichier en parquet") {
    Given("Je dois lire le fichier")
    val input = "src/main/resources/departements-france.csv"

    When("Je lis le fichier")
    HelloWorld.main(input)

    Then("J'ai lu le fichier")
  }

}

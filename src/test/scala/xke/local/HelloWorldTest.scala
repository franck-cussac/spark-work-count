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
    Given("dataframe avec 3 colonnes : nom région, code région, num dept")
    val input = spark.sparkContext.parallelize(
      List(
        ("Grand Est", 44 , 52),
        ("Grand Est", 44 , 52),
        ("Grand Est", 44 , 52),
        ("Guadeloupe", 1 , 50)
      )
    ).toDF("nom_region", "code_region", "code_departement")

    val expected = spark.sparkContext.parallelize(
      List(
        (44 , 52, "Grand Est"),
        ( 1 , 50, "Guadeloupe")
      )
    ).toDF("code_region", "avg(code_departement)", "nom_region")
    val actual = HelloWorld.avgDepByReg(input)
    assertDataFrameEquals(actual, expected)

  }

  test("je veux renommer la colonne des moyennes des numéros département") {

    Given("dataframe avec 3 colonnes : nom région, code région, moyenne des départements")
    val input = spark.sparkContext.parallelize(
      List(
        (44 , 52, "Grand Est"),
        ( 1 , 50, "Guadeloupe")
      )
    ).toDF("code_region", "avg(code_departement)", "nom_region")

    val expected = spark.sparkContext.parallelize(
      List(
        (44 , 52, "Grand Est"),
        ( 1 , 50, "Guadeloupe")
      )
    ).toDF("code_region", "avg_dep", "nom_region")

    val actual = HelloWorld.renameColumn(input)
    assertDataFrameEquals(actual, expected)
  }



  test("je veux vérifier que je lis un fichier, ajoute une colonne, la renomme, et sauvegarde mon fichier en parquet") {
    val input = "src/test/resources/departements-france.csv"

    val expected = spark.sparkContext.parallelize(
      List(
        (94 , 1, "Corse"),
        (76 , 2, "Occitanie")
      )
    ).toDF("code_region", "avg_dep", "nom_region")

    val df = spark.read.option("sep", ",").option("header", true).csv(input)
    HelloWorld.renameColumn(HelloWorld.avgDepByReg(df)).write.mode("overwrite").parquet("parquet")
    val actually = spark.sqlContext.read.parquet("parquet")

    assertDataFrameEquals(actually, expected)
  }

  }

}

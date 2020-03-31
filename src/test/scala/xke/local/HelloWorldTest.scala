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
    Given("une dataframe avec au moins 3 colonnes : nom région, code région et numéro département")
    val input = spark.sparkContext.parallelize(
      List(
        (1, 2, "toto"),
        (1, 3, "toto"),
        (1, 4, "toto"),
        (2, 14, "zaza"),
        (2, 54, "zaza"),
        (2, 7, "zaza"),
        (2, 9, "zaza")
      )
    ).toDF("code_region", "code_departement", "nom_region")

    val expected = spark.sparkContext.parallelize(
      List(
        (1, 3, "toto"),
        (2, 21, "zaza")
      )
    ).toDF("code_region", "avg(code_departement)", "nom_region")

    When("On regroupe les donnée par code_region, par nom_region et on fait un average du code_departement")
    val actual = HelloWorld.avgDepByReg(input)

    Then("On option un dataframe avec 3 colone, le nom région, le code région et l'avg du numéro département")
    assertDataFrameEquals(actual, expected)

  }

  test("je veux renommer la colonne des moyennes des numéros département") {

    Given("une dataframe avec au moins 3 colonnes : nom région, code région et numéro département")
    val input = spark.sparkContext.parallelize(
      List(
        (1, 19, "toto"),
        (2, 52, "tutu")
      )
    ).toDF("code_region","avg(code_departement)", "nom_region")
    val expected = spark.sparkContext.parallelize(
      List(
        (1, 19, "toto"),
        (2, 52, "tutu")
      )
    ).toDF("code_region", "avg_dep", "nom_region")

    When("")
    val actual = HelloWorld.renameColumn(input)

    Then("")
    assertDataFrameEquals(actual, expected)
  }

  test("je veux vérifier que je lis un fichier, ajoute une colonne, la renomme, et sauvegarde mon fichier en parquet") {

    Given("Je fait des opération de transformation d'ajout de colonne et de renommage avant de générer mon parquet")
    //HelloWorld.main(null)

    When("Je lit mon fichier parquet")
    val input = spark.read.parquet("/home/ubuntu/workspace/hadoop/spark-work-count/src/main/resources/parquet")

    Then("J'ai le bon nombre de colonne dans mon fichier")
    val expected = 3
    val actual = assert(input.columns.length === expected)

  }

}

package xke.local

import javax.annotation.meta
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
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
    Given("Une data frame")
    val input = spark.sparkContext.parallelize(
      List(
        (1, 64, "Auvergne-Rhône-Alpes"),
        (1, 24, "Auvergne-Rhône-Alpes"),
        (1, 95, "Auvergne-Rhône-Alpes"),
        (2, 75, "iles-de-france"),
        (2, 56, "iles-de-france"),
        (2, 46, "iles-de-france")

      )).toDF( "code_region", "code_departement", "nom_region")

    val expected = spark.sparkContext.parallelize(
      List(
        (1, 61, "Auvergne-Rhône-Alpes"),
        (2, 59, "iles-de-france")
      )).toDF("code_region", "avg(code_departement)", "nom_region")

    When("J'applique ma fonction avgDepByReg je dois avoir une colonne avec la moyenne des numéros département par région")
    val actual =   HelloWorld.avgDepByReg(input)
    assertDataFrameEquals(actual, expected);
  }

  test("je veux renommer la colonne des moyennes des numéros département") {

    Given("une dataframe avec au moins 3 colonnes : nom région, code région et numéro département")
    val input = spark.sparkContext.parallelize(
      List(
        (1, 61, "Auvergne-Rhône-Alpes"),
        (2, 59, "iles-de-france")
      )).toDF("code_region", "avg(code_departement)", "nom_region")

    val expected = spark.sparkContext.parallelize(
      List(
        (1, 61, "Auvergne-Rhône-Alpes"),
        (2, 59, "iles-de-france")
      )).toDF("code_region", "avg_dep", "nom_region")

    When("On renme la colonne 'avg(code_departement)' avec 'avg_dep'")
    val actual = HelloWorld.renameColumn(input, "avg(code_departement)", "avg_dep")

    Then("La colonne 'avg(code_departement)' devra être renommée avec 'avg_dep'")

    assertDataFrameEquals(actual, expected)
    val columnsSet =    actual.columns.toSet
    columnsSet shouldEqual  Set("code_region", "avg_dep", "nom_region")
    columnsSet should not contain "avg(code_departement)"
  }

  test("je veux vérifier que je lis un fichier, ajoute une colonne, la renomme, et sauvegarde mon fichier en parquet") {
    Given("Un chemin vers un jeu de de données et un chemin où stocker le résultat")
    val spark =  SparkSession.builder().master("local[*]").appName("test").getOrCreate()
    val pathResult = "result"
    val pathData   = "src/main/resources/departements-france.csv"
    val args = Array(pathData, pathResult)

    When("On excute le main")
    HelloWorld.main(args)
    val df = spark.read.parquet(pathResult)

    Then("Il faut qu'on trouve la nouvelle colonne 'avg_dep' et ne pas trouver la colonne 'avg(code_departement)'")
    val columnsSet =    df.columns.toSet
    columnsSet shouldEqual  Set("code_region", "avg_dep", "nom_region")
    columnsSet should not contain "avg(code_departement)"
    df.select("avg_dep").count() should not equal(0)
  }

}

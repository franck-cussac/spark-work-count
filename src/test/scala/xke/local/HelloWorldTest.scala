package xke.local

import org.apache.spark.sql.SparkSession
import org.scalatest.{FunSuite, GivenWhenThen}
import spark.{DataFrameAssertions, SharedSparkSession}

class HelloWorldTest extends FunSuite with GivenWhenThen with DataFrameAssertions {
  val spark: SparkSession = SharedSparkSession.sparkSession

  import spark.implicits._

  test("je veux ajouter une colonne avec la moyenne des numéros département par région") {
    Given("un dataframe avec une région et deux département pour une moyenne")

    val input = spark.sparkContext.parallelize(
      List(
        ("12", "Ile de france", 75),
        ("10", "Ile de france", 75),
        ("8", "Ile de france", 75),
        ("5", "Auvergne", 70)
      )
    ).toDF("code_departement", "nom_region", "code_region")

    When("j'appelle la fonction de moyenne")

    val actual = HelloWorld.getAverageDepartmentNumberByRegion(input)

    Then("les dataframes doivent être les mêmes")

    val expected = spark.sparkContext.parallelize(
      List(
        (75, 10, "Ile de france"),
        (70, 5, "Auvergne")
      )
    ).toDF("code_region", "avg(code_departement)", "nom_region")

    assertDataFrameEquals(actual, expected)

  }

  test("je veux renommer la colonne des moyennes des numéros département") {

    Given("un dataframe avec la colonne avg")
    val input = spark.sparkContext.parallelize(
      List(
        (75, 10, "Ile de france"),
        (70, 5, "Auvergne")
      )
    ).toDF("code_region", "avg(code_departement)", "nom_region")

    When("j'appelle la fonction de renommage")
    val actual = HelloWorld.renameAverageColumn(input)

    Then("les dataframes doivent être les mêmes")
    val expected = spark.sparkContext.parallelize(
      List(
        (75, 10, "Ile de france"),
        (70, 5, "Auvergne")
      )
    ).toDF("code_region", "avg_dep", "nom_region")

    assertDataFrameEquals(actual, expected)
  }

  test("je veux vérifier que je lis un fichier, ajoute une colonne, la renomme, et sauvegarde mon fichier en parquet") {
    Given("un dataframe rétréci provenant d'un fichier")
    spark.read.option("delimiter", ",").option("header", value = true).csv("src/main/resources/departements-france-small.csv")

    val expected = spark.sparkContext.parallelize(
      List(
        (84, 1, "Auvergne-Rhone-Alpes"),
        (32, 2, "Hauts-de-France")
      )
    ).toDF("code_region", "avg_dep", "nom_region")

    When("j'appelle la fonction principale")
    HelloWorld.main(null)
    val main = spark.read.parquet("src/main/parquet/exo.parquet")

    Then("les dataframes doivent être les mêmes")
    assertDataFrameEquals(main, expected)
  }

}

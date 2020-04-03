package xke.local

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.{FunSuite, GivenWhenThen}
import spark.{DataFrameAssertions, SharedSparkSession}

class HelloWorldTest extends FunSuite with GivenWhenThen with DataFrameAssertions {
  val sparkSession: SparkSession = SharedSparkSession.sparkSession

  import sparkSession.implicits._

  test("ajout de colonne avec moyenne des numéros de département par région") {
    Given("dataframe avec les premiers départements, le nom département, la région et nom région")
    val input = List(
      ("01", "Ain", 84, "Auvergne-Rhône-Alpes"),
      ("02", "Aisne", 32, "Hauts-de-France"),
      ("03", "Allier", 84, "Auvergne-Rhône-Alpes"),
      ("04", "Alpes-de-Haute-Provence", 93, "Provence-Alpes-Côte d'Azur")
    ).toDF("code_departement", "nom_departement", "code_region", "nom_region")

    When("j'appelle la fonction de moyenne")
    val actual = HelloWorld.getAverageDepartmentNumberByRegion(input)

    val expected = List(
      (32, "Hauts-de-France", 2.0),
      (84, "Auvergne-Rhône-Alpes", 2.0),
      (93, "Provence-Alpes-Côte d'Azur", 4.0)
    ).toDF("code_region", "nom_region", "avg(code_departement)")

    assertDataFrameEquals(actual, expected)
  }

  test("je renomme avg(code_departement) en avg_dep") {
    Given("dataframe avec les premiers code région, nom région et moyenne du numéro de département")
    val input = List(
      (32, "Hauts-de-France", 2.0),
      (84, "Auvergne-Rhône-Alpes", 2.0),
      (93, "Provence-Alpes-Côte d'Azur", 4.0)
    ).toDF("code_region", "nom_region", "avg(code_departement)")

    When("j'appelle le renommage")
    val actual = HelloWorld.renameAverageColumn(
      input)

    val expected = List(
      (32, "Hauts-de-France", 2.0),
      (84, "Auvergne-Rhône-Alpes", 2.0),
      (93, "Provence-Alpes-Côte d'Azur", 4.0)
    ).toDF("code_region", "nom_region", "avg_dep")

    assertDataFrameEquals(actual, expected)
  }

  test("je veux créer mon fichier parquet") {
    Given("dataframe avec les premiers départements, le nom département, la région et nom région")
    val parquetLocation = "src/test/resources/parquet"

    val input = sparkSession.sparkContext.parallelize(List(
      ("01", "Ain", 84, "Auvergne-Rhône-Alpes"),
      ("02", "Aisne", 32, "Hauts-de-France"),
      ("03", "Allier", 84, "Auvergne-Rhône-Alpes"),
      ("04", "Alpes-de-Haute-Provence", 93, "Provence-Alpes-Côte d'Azur")
    )).toDF("code_departement", "nom_departement", "code_region", "nom_region")

    When("je renomme, calcule la moyenne puis créer le fichier parquet")
    HelloWorld.renameAverageColumn(
      HelloWorld.getAverageDepartmentNumberByRegion(input)
    ).write.mode(SaveMode.Overwrite).parquet(parquetLocation)

    val output = sparkSession.sqlContext.read.parquet(parquetLocation)

    val expected = List(
      (32, "Hauts-de-France", 2.0),
      (84, "Auvergne-Rhône-Alpes", 2.0),
      (93, "Provence-Alpes-Côte d'Azur", 4.0)
    ).toDF("code_region", "nom_region", "avg_dep")

    assertDataFrameEquals(output, expected)
  }
}

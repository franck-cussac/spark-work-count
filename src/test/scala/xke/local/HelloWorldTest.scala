package xke.local

import org.scalatest.{FunSuite, GivenWhenThen}
import spark.{DataFrameAssertions, SharedSparkSession}

class HelloWorldTest extends FunSuite with GivenWhenThen with DataFrameAssertions {
  val spark = SharedSparkSession.sparkSession
  import spark.implicits._

  test("main must create a file with word count result") {
    Given("input filepath and output filepath")
    val input = "src/test/resources/input/v1/input.txt"
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
  }

  test("Je veux ajouter une colonne avec la moyenne des numéros de département par région") {
    Given("Une dataframe avec 4 colonnes : code département, nom département, code région et nom région")
    val inputDf = spark.sparkContext.parallelize(List(
      ("01", "Ain", 84, "Auvergne-Rhône-Alpes"),
      ("02", "Aisne", 32, "Hauts-de-France"),
      ("03", "Allier", 84, "Auvergne-Rhône-Alpes"),
      ("04", "Alpes-de-Haute-Provence", 93, "Provence-Alpes-Côte d'Azur")
    )).toDF("code_departement", "nom_departement", "code_region", "nom_region")

    When("")
    val actualDf = HelloWorld.avgDepByReg(inputDf)

    Then("")
    val expectedDf = spark.sparkContext.parallelize(List(
      (32, "Hauts-de-France", 2.0),
      (84, "Auvergne-Rhône-Alpes", 2.0),
      (93, "Provence-Alpes-Côte d'Azur", 4.0)
    )).toDF("code_region", "nom_region", "avg(code_departement)")

    assertDataFrameEquals(actualDf, expectedDf)
  }

  test("Je veux renommer avg(code_departement) en avg_dep") {
    Given("Une dataframe avec 3 colonnes : code région, nom région et moyenne du numéro de département")
    val inputDef = spark.sparkContext.parallelize(List(
      (32, "Hauts-de-France", 2.0),
      (84, "Auvergne-Rhône-Alpes", 2.0),
      (93, "Provence-Alpes-Côte d'Azur", 4.0)
    )).toDF("code_region", "nom_region", "avg(code_departement)")

    When("")
    val actualDf = HelloWorld.renameColumn(inputDef, "avg(code_departement)", "avg_dep")

    val expectedDf = spark.sparkContext.parallelize(List(
      (32, "Hauts-de-France", 2.0),
      (84, "Auvergne-Rhône-Alpes", 2.0),
      (93, "Provence-Alpes-Côte d'Azur", 4.0)
    )).toDF("code_region", "nom_region", "avg_dep")

    Then("")
    assertDataFrameEquals(actualDf, expectedDf)
  }

  test("Je veux vérifier que quand je lis un fichier, ajoute une colonne, la renomme et sauvegarde mon fichier en parquet") {
    Given("Une dataframe avec 4 colonnes : code département, nom département, code région et nom région")
    val output = "src/test/resources/output/v2/parquet"

    val inputDf = spark.sparkContext.parallelize(List(
      ("01", "Ain", 84, "Auvergne-Rhône-Alpes"),
      ("02", "Aisne", 32, "Hauts-de-France"),
      ("03", "Allier", 84, "Auvergne-Rhône-Alpes"),
      ("04", "Alpes-de-Haute-Provence", 93, "Provence-Alpes-Côte d'Azur")
    )).toDF("code_departement", "nom_departement", "code_region", "nom_region")

    When("")
    val actualDf = HelloWorld.renameColumn(HelloWorld.avgDepByReg(inputDf), "avg(code_departement)", "avg_dep")
    actualDf.write.format("parquet").mode("overwrite").save(output)

    Then("I can read output file and find my values")
    val outputDf = spark.sqlContext.read.parquet(output)

    val expectedDf = spark.sparkContext.parallelize(List(
      (32, "Hauts-de-France", 2.0),
      (84, "Auvergne-Rhône-Alpes", 2.0),
      (93, "Provence-Alpes-Côte d'Azur", 4.0)
    )).toDF("code_region", "nom_region", "avg_dep")

    assertDataFrameEquals(outputDf, expectedDf)
  }
}

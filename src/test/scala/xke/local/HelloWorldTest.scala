package xke.local

import org.apache.spark.sql.SaveMode
import org.scalatest.{FunSuite, GivenWhenThen}
import spark.{DataFrameAssertions, SharedSparkSession}

class HelloWorldTest extends FunSuite with GivenWhenThen with DataFrameAssertions {
  val spark = SharedSparkSession.sparkSession
  import spark.implicits._

  test("Je veux pouvoir récupérer l'entier dans un code département") {
    Given("Une liste de chaîne de caractères")
    val input = List("110", "011", "2A", "2B")

    When("J'appelle extractCode")
    val actual = input.map(HelloWorld.extractCode)

    Then("Une liste d'entier")
    val expected = List(110, 11, 2, 2)

    actual should contain theSameElementsAs expected
  }

  test("Je veux ajouter une colonne avec la moyenne des numéros de département par région") {
    Given("Une dataframe avec 4 colonnes : code département, nom département, code région et nom région")
    val inputDf = List(
      ("01", "Ain", 84, "Auvergne-Rhône-Alpes"),
      ("02", "Aisne", 32, "Hauts-de-France"),
      ("03", "Allier", 84, "Auvergne-Rhône-Alpes"),
      ("04", "Alpes-de-Haute-Provence", 93, "Provence-Alpes-Côte d'Azur")
    ).toDF("code_departement", "nom_departement", "code_region", "nom_region")

    When("J'appelle avgDepByReg")
    val actualDf = HelloWorld.avgDepByReg(inputDf)

    Then("Un dataframe avec 3 colonnes : code_region, nom_region et avg(code_department)")
    val expectedDf = List(
      (32, "Hauts-de-France", 2.0),
      (84, "Auvergne-Rhône-Alpes", 2.0),
      (93, "Provence-Alpes-Côte d'Azur", 4.0)
    ).toDF("code_region", "nom_region", "avg(code_departement)")

    assertDataFrameEquals(actualDf, expectedDf)
  }

  test("Je veux renommer avg(code_departement) en avg_dep") {
    Given("Un dataframe avec 3 colonnes : code région, nom région et moyenne du numéro de département")
    val inputDef = List(
      (32, "Hauts-de-France", 2.0),
      (84, "Auvergne-Rhône-Alpes", 2.0),
      (93, "Provence-Alpes-Côte d'Azur", 4.0)
    ).toDF("code_region", "nom_region", "avg(code_departement)")

    When("J'appelle renameColumns")
    val actualDf = HelloWorld.renameColumns(
      inputDef,
      Map("avg(code_departement)" -> "avg_dep")
    )

    val expectedDf = List(
      (32, "Hauts-de-France", 2.0),
      (84, "Auvergne-Rhône-Alpes", 2.0),
      (93, "Provence-Alpes-Côte d'Azur", 4.0)
    ).toDF("code_region", "nom_region", "avg_dep")

    Then("Un dataframe avec 3 colonnes : code_region, nom_region et avg_dep")
    assertDataFrameEquals(actualDf, expectedDf)
  }

  test("Je veux vérifier que quand je lis un fichier, ajoute une colonne, la renomme et sauvegarde mon fichier en parquet") {
    Given("Un dataframe avec 4 colonnes : code département, nom département, code région et nom région")
    val output = "src/test/resources/output/v2/parquet"

    val inputDf = spark.sparkContext.parallelize(List(
      ("01", "Ain", 84, "Auvergne-Rhône-Alpes"),
      ("02", "Aisne", 32, "Hauts-de-France"),
      ("03", "Allier", 84, "Auvergne-Rhône-Alpes"),
      ("04", "Alpes-de-Haute-Provence", 93, "Provence-Alpes-Côte d'Azur")
    )).toDF("code_departement", "nom_departement", "code_region", "nom_region")

    When("Je calcule la moyenne par région et que je renomme la colonne avg(code_department)")
    HelloWorld.renameColumns(
      HelloWorld.avgDepByReg(inputDf),
      Map("avg(code_departement)" -> "avg_dep")
    ).write.mode(SaveMode.Overwrite).parquet(output)

    Then("I can read output file and find my values")
    val outputDf = spark.sqlContext.read.parquet(output)

    val expectedDf = List(
      (32, "Hauts-de-France", 2.0),
      (84, "Auvergne-Rhône-Alpes", 2.0),
      (93, "Provence-Alpes-Côte d'Azur", 4.0)
    ).toDF("code_region", "nom_region", "avg_dep")

    assertDataFrameEquals(outputDf, expectedDf)
  }
}

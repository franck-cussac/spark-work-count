package xke.local

import org.scalatest.{FunSuite, GivenWhenThen}
import spark.{DataFrameAssertions, SharedSparkSession}

class HelloWorldTest extends FunSuite with GivenWhenThen with DataFrameAssertions {
  val spark = SharedSparkSession.sparkSession
  import spark.implicits._

  test("Je veux ajouter une colonne avec la moyenne des numéros de département par région") {
    Given("")
    val input = spark.sparkContext.parallelize(List(
      ("01", "Ain", 84, "Auvergne-Rhône-Alpes"),
      ("02", "Aisne", 32, "Hauts-de-France"),
      ("03", "Allier", 84, "Auvergne-Rhône-Alpes"),
      ("04", "Alpes-de-Haute-Provence", 93, "Provence-Alpes-Côte d'Azur")
    )).toDF("code_departement", "nom_departement", "code_region", "nom_region")

    When("")
    val actual = HelloWorld.avgDepByReg(input)

    Then("")
    val expected = spark.sparkContext.parallelize(List(
      (32, "Hauts-de-France", 2.0),
      (84, "Auvergne-Rhône-Alpes", 2.0),
      (93, "Provence-Alpes-Côte d'Azur", 4.0)
    )).toDF("code_region", "nom_region", "avg(code_departement)")

    assertDataFrameEquals(actual, expected)
  }

  test("Je veux renommer la colonne des moyennes des numéros de départements") {
    Given("")
    val input = spark.sparkContext.parallelize(List(
      (32, "Hauts-de-France", 2.0),
      (84, "Auvergne-Rhône-Alpes", 2.0),
      (93, "Provence-Alpes-Côte d'Azur", 4.0)
    )).toDF("code_region", "nom_region", "avg(code_departement)")

    When("")
    val actual = HelloWorld.renameColumn(input, "avg_dep")

    Then("")
    val expected = spark.sparkContext.parallelize(List(
      (32, "Hauts-de-France", 2.0),
      (84, "Auvergne-Rhône-Alpes", 2.0),
      (93, "Provence-Alpes-Côte d'Azur", 4.0)
    )).toDF("code_region", "nom_region", "avg_dep")

    assertDataFrameEquals(actual, expected)
  }

  test("Je veux vérifier que quand je lis un fichier, ajoute une colonne, la renomme et sauvegarde mon fichier en parquet") {
    Given("")
    val output = "src/test/resources/output/v2/parquet"
    val inputDf = spark.sparkContext.parallelize(List(
      ("01", "Ain", 84, "Auvergne-Rhône-Alpes"),
      ("02", "Aisne", 32, "Hauts-de-France"),
      ("03", "Allier", 84, "Auvergne-Rhône-Alpes"),
      ("04", "Alpes-de-Haute-Provence", 93, "Provence-Alpes-Côte d'Azur")
    )).toDF("code_departement", "nom_departement", "code_region", "nom_region")

    When("")
    val actualDf = HelloWorld.renameColumn(HelloWorld.avgDepByReg(inputDf), "avg_dep")
    actualDf.write.format("parquet").mode("overwrite").save(output)

    Then("")
    val outputDf = spark.sqlContext.read.parquet(output)
    val expectedDf = spark.sparkContext.parallelize(List(
      (32, "Hauts-de-France", 2.0),
      (84, "Auvergne-Rhône-Alpes", 2.0),
      (93, "Provence-Alpes-Côte d'Azur", 4.0)
    )).toDF("code_region", "nom_region", "avg_dep")

    assertDataFrameEquals(outputDf, expectedDf)
  }

  test("Je veux vérifier que la colonne code_departement soit en integer (suppression des 0 et des lettres)") {
    Given("")
    val s = "0azerty78A"
    val expected = 78

    When("")
    val actual = HelloWorld.parseToInt(s)

    Then("")
    assert(actual === expected)
  }

  test("Je veux faire la jointure entre les départements et les villes") {
    Given("")
    val inputDepartements = List(
      (1, 2, "toto"),
      (1, 3, "toto"),
      (1, 4, "toto"),
      (2, 15, "tata"),
      (2, 16, "tata"),
      (2, 17, "tata"),
      (2, 18, "tata")
    ).toDF("code_region", "code_departement", "nom_region")

    val inputCities = List(
      (2, "Nantes"),
      (15, "Reims"),
      (16, "Charleville"),
      (17, "Sedan")
    ).toDF("department_code", "name")

    val expected = List(
      (2, "Nantes", 1, "toto"),
      (3, null, 1, "toto"),
      (4, null, 1, "toto"),
      (15, "Reims", 2, "tata"),
      (16, "Charleville", 2, "tata"),
      (17, "Sedan", 2, "tata"),
      (18, null, 2, "tata")
    ).toDF("code_departement", "name", "code_region", "nom_region")

    When("")
    val actual = HelloWorld.joinDf(inputDepartements, inputCities)

    Then("")
    assertDataFrameEquals(actual, expected)
  }

}

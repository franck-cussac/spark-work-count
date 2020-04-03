package xke.local

import org.scalatest.{FunSuite, GivenWhenThen}
import spark.{DataFrameAssertions, SharedSparkSession}

class HelloWorldTest extends FunSuite with GivenWhenThen with DataFrameAssertions {
  val spark = SharedSparkSession.sparkSession
  import spark.implicits._

  test("je veux ajouter une colonne avec la moyenne des numéros département par région") {
    Given("une dataframe avec au moins 3 colonnes : code_region, code-departement, et nom_region ")
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
    val actual = HelloWorld.avgDepByReg(input)
    assertDataFrameEquals(actual, expected);
  }

  test("je veux renommer la colonne des moyennes des numéros département") {
    val input = spark.sparkContext.parallelize(List(
      (1, 3.0, "toto"),
      (2, 20.0, "zaza")
    )).toDF("code_region", "avg(code_departement)", "nom_region")

    val expected = spark.sparkContext.parallelize(List(
      (1, 3.0, "toto"),
      (2, 20.0, "zaza")
    )).toDF("code_region", "avg_dep", "nom_region")

    When("on renomme la colonne des moyennes")
    val actual = HelloWorld.renameColumn(input)

    Then("Je doit avoir la colonne renommée")
    assertDataFrameEquals(actual, expected)
  }

  test("je veux vérifier que je lis un fichier, ajoute une colonne, la renomme, et sauvegarde mon fichier en parquet") {
    Given("a dataframe from file")
    spark.read.option("sep", ",").option("header", true).csv("src/main/resources/departements-france-mock.csv")

    val expected = spark.sparkContext.parallelize(
      List(
        (52,"Pays de la Loire", 60.6 ),
        (32,"Hauts-de-France", 52.6 )
      )
    ).toDF("code_region", "nom_region", "avg_dep")

    When("I call main")
    HelloWorld.main(Array("src/main/resources/departements-france-mock.csv"))
    val main = spark.read.option("delimiter", ",").option("header", true).parquet("ParquetResult")

    Then("result")
    main.show()
    expected.show()
    assertDataFrameEquals(main, expected)

  }

  test("Je veut convertir une chaine de caractère en nombre") {
    Given("Chaine de caractère composée de chiffre et de lettres")
    val input = "001ab55"
    When("Lorsque la fonction s'execute")
    val expected = 155
    val actual = HelloWorld.string2Int(input)
    Then("J'ai le bon résultat")
    assert(actual === expected)
  }
}

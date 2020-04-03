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
        (1, 61),
        (2, 59)
      )).toDF("code_region", "avg(code_departement)")

    When("J'applique ma fonction avgDepByReg je dois avoir une colonne avec la moyenne des numéros département par région")
    val actual =   HelloWorld.avgDepByReg(input)
    assertDataFrameEquals(actual, expected);
  }

  test("Je veux renommer la colonne des moyennes des numéros département") {
    val input = spark.sparkContext.parallelize(List(
      (1, 3.0, "toto"),
      (2, 20.0, "zaza")
    )).toDF("code_region", "avg(code_departement)", "nom_region")

    val expected = spark.sparkContext.parallelize(List(
      (1, 3.0, "toto"),
      (2, 20.0, "zaza")
    )).toDF("code_region", "avg_dep", "nom_region")

    When("Je veux renommer la colonne des moyennes.")
    val actual = HelloWorld.renameColumn(input, columnName = "avg(code_departement)", newName = "avg_dep")

    Then("Je avoir la colonne renommée")
    assertDataFrameEquals(actual, expected)
  }
  test("je veux vérifier que je lis un fichier, ajoute une colonne, la renomme, et sauvegarde mon fichier en parquet") {
    Given("a dataframe from file")
    spark.read.option("sep", ",").option("header", true).csv("src/main/resources/departements-france.csv")

    val expected = spark.sparkContext.parallelize(
      List(
        (84 , 1, "Auvergne-Rhone-Alpes"),
        (32 , 2, "Hauts-de-France")
      )
    ).toDF("code_region", "avg_dep", "nom_region")

    When("I call main")
    HelloWorld.main(null)
    val main = HelloWorld.writeParquet(dataFrame = expected, "ParquetResult")
    main.show
    expected.show
    Then("result")
    assertDataFrameEquals(main, expected)
  }

  test("Je veut convertir une chaine de caractère en nombre") {
    Given("Une chaine de caractère composée de chiffre et de lettres")
    val input = "045d45"
    When("Quand j'exécute la fonction")
    val expected = 4545
    val actual = HelloWorld.stringToInt(input)
    Then("J'ai le résultat attendu")
    assert(actual === expected)
  }

  test("Je veux join avec udf") {
    Given("2 DFs")
    val df1 = spark.sparkContext.parallelize(
      List(
        ("valeur1", "01"),
        ("valeur2", "A2")
      )
    ).toDF("col1", "col2")
    val df2 = spark.sparkContext.parallelize(
      List(
        ("1", "valeur3"),
        ("02A", "valeur4")
      )
    ).toDF("col2", "col3")

    When("I call the function")
    val res = HelloWorld.udfJoin(df1, "col2", df2)

    Then("Je vérifie que les df sont égales")
    val expected = spark.sparkContext.parallelize(
      List(
        ("valeur1", 1, "valeur3"),
        ("valeur2", 2, "valeur4")
      )
    ).toDF("col1", "col2", "col3")

    assertDataFrameEquals(res, expected)
  }
}
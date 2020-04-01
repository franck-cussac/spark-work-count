package xke.local

import javax.annotation.meta
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import org.scalatest.{FunSuite, GivenWhenThen}
import spark.{DataFrameAssertions, SharedSparkSession}

class HelloWorldTest extends FunSuite with GivenWhenThen with DataFrameAssertions {
  val spark: SparkSession = SharedSparkSession.sparkSession
  import spark.implicits._

  test("Je veux ajouter une colonne avec la moyenne des numéros département par région") {
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


  test("Test si la fonction 'toInteger' supprime les caractères et garde que les chiffres"){
    val input = "t45"
    val expected = 45

    val actual = HelloWorld.toInteger(input)

    actual shouldEqual(expected)
  }

  test("TTest si la fonction 'toInteger' supprime les 0 au début des chiffres"){
    val input = "t0efrf4f5Y"
    val expected = 45

    val actual = HelloWorld.toInteger(input)

    actual shouldEqual(expected)
  }

  test("Test si la fonction 'toInteger' garde le 0 à la fin du résultat"){
    val input = "t0efrf4f5Y0"
    val expected = 450

    val actual = HelloWorld.toInteger(input)

    actual shouldEqual(expected)
  }


  test("je veux faire la jointure entre les départements et les villes") {
    Given("Les départements et les villes, et ce que je souhaite en sortie")
    val inputDepts = List(
      (1, 2, "toto"),
      (1, 3, "toto"),
      (1, 4, "toto"),
      (2, 14, "zaza"),
      (2, 54, "zaza"),
      (2, 7, "zaza"),
      (2, 5, "zaza")
    ).toDF("code_region", "code_departement", "nom_region")
    val inputCities = List(
      (2, "Paris"),
      (3, "Lille"),
      (4, "Lyon"),
      (5, "Bordeaux")
    ).toDF("department_code", "name")
    val expected = List(
      (2, "Paris", 1, "toto"),
      (3, "Lille", 1, "toto"),
      (4, "Lyon", 1, "toto"),
      (5, "Bordeaux", 2, "zaza"),
      (14, null, 2, "zaza"),
      (54, null, 2, "zaza"),
      (7, null, 2, "zaza")
    ).toDF("code_departement", "name", "code_region", "nom_region")

    When("Je fais la jointure")
    val actual = HelloWorld.joinCitiesAndDepartment(inputDepts, inputCities)

    Then("La jointure devrait être correcte")
    assertDataFrameEquals(actual, expected)
  }

  //Test d'intégration
  test("je veux vérifier que je lis un fichier, ajoute une colonne, la renomme, et sauvegarde mon fichier en parquet") {
    Given("Un chemin vers un jeu de de données et un chemin où stocker le résultat")
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

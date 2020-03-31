package xke.local

import org.scalatest.{FunSuite, GivenWhenThen}
import spark.{DataFrameAssertions, SharedSparkSession}

class HelloWorldTest extends FunSuite with GivenWhenThen with DataFrameAssertions {
  val spark = SharedSparkSession.sparkSession
  import spark.implicits._

  test("je veux ajouter une colonne avec la moyenne des numéros département par région") {
    Given("Les données d'entrées et ce que je souhaite en sortie")
    val input = spark.sparkContext.parallelize(List(
      (1, 2, "toto"),
      (1, 3, "toto"),
      (1, 4, "toto"),
      (2, 14, "zaza"),
      (2, 54, "zaza"),
      (2, 7, "zaza"),
      (2, 5, "zaza")
    )).toDF("code_region", "code_departement", "nom_region")
    val expected = spark.sparkContext.parallelize(List(
      (1, 3.0, "toto"),
      (2, 20.0, "zaza")
    )).toDF("code_region", "avg(code_departement)", "nom_region")

    When("on calcule la moyenne des numéros de départements par région")
    val actual = HelloWorld.avgDepByReg(input)

    Then("Je dois retrouver les moyennes par régions des départements")
    assertDataFrameEquals(actual, expected)
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

    Then("Je avoir la colonne renommée")
    assertDataFrameEquals(actual, expected)
}

  test("je veux vérifier que je lis un fichier, ajoute une colonne, la renomme, et sauvegarde mon fichier en parquet") {
    Given("Les données d'entrées et ce que je souhaite en sortie")
    val input = spark.sparkContext.parallelize(List(
      (1, 2, "toto"),
      (1, 3, "toto"),
      (1, 4, "toto"),
      (2, 14, "zaza"),
      (2, 54, "zaza"),
      (2, 7, "zaza"),
      (2, 5, "zaza")
    )).toDF("code_region", "code_departement", "nom_region")
    val expected = spark.sparkContext.parallelize(List(
      (1, 3.0, "toto"),
      (2, 20.0, "zaza")
    )).toDF("code_region", "avg_dep", "nom_region")

    When("on calcule les moyennes, on renomme la colonne des moyennes, on écrit, puis on lit")
    HelloWorld.main(Array[String]("test"))
    val actual = spark.read.parquet("src/main/parquets/output.parquet")

    Then("On devrait obtenir le DF après le renommage")
    assertDataFrameEquals(actual, expected)
  }
}

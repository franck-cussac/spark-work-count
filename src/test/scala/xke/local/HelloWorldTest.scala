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
    val actual =   HelloWorld.avgDepByReg(input)
    assertDataFrameEquals(actual, expected);
  } //PROF

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

    Given("Je fait des opération de transformation d'ajout de colonne et de renommage avant de générer mon parquet")
    //HelloWorld.main(null)

    When("Je lit mon fichier parquet")
    val input = spark.read.parquet("src/main/parquet/ex1.parquet")

    Then("J'ai le bon nombre de colonne dans mon fichier")
    val expected = 3
    val actual = assert(input.columns.length === expected)

  }

  test("Je veut convertir une chaine de caractère en nombre") {
    Given("Une chaine de caractère composée de chiffre et de lettres")
    val input = "045d45"
    When("Quand jexécute la fonction")
    val expected = 4545
    val actual = HelloWorld.stringToInt(input)
    Then("J'ai le résultat attendu")
    assert(actual === expected)
  }
}
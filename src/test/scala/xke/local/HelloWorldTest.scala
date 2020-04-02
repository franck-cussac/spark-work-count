package xke.local

import org.scalatest.{FunSuite, GivenWhenThen}
import spark.{DataFrameAssertions, SharedSparkSession}

class HelloWorldTest extends FunSuite with GivenWhenThen with DataFrameAssertions {
  val spark = SharedSparkSession.sparkSession
  import spark.implicits._

  test("je veux ajouter une colonne avec la moyenne des numéros département par région") {

    Given("une dataframe avec au moins 3 colonnes : code région, numéro département et nom région")
    val input = spark.sparkContext.parallelize(
      List(
        (84, 2, "Auvergne-Rhône-Alpes"),
        (84, 3, "Auvergne-Rhône-Alpes"),
        (84, 4, "Auvergne-Rhône-Alpes"),
        (32, 14, "Hauts-de-France"),
        (32, 54, "Hauts-de-France"),
        (32, 7, "Hauts-de-France"),
        (32, 9, "Hauts-de-France")
      )
    ).toDF("code_region", "code_departement", "nom_region")

    val expected = spark.sparkContext.parallelize(
      List(
        (84, 3, "Auvergne-Rhône-Alpes"),
        (32, 21, "Hauts-de-France")
      )
    ).toDF("code_region", "avg(code_departement)", "nom_region")

    When("calcul de la moyenne")
    val actual = HelloWorld.avgDepByReg(input)

    Then("je dois avoir les moyennes par régions")
    assertDataFrameEquals(actual, expected)
  }

  test("je veux renommer la colonne des moyennes des numéros département") {

    Given("une dataframe avec au moins 3 colonnes : code région, avg(code_departement) et nom région")
    val input = spark.sparkContext.parallelize(
      List(
        (84, 3, "Auvergne-Rhône-Alpes"),
        (32, 21, "Hauts-de-France")
      )
    ).toDF("code_region", "avg(code_departement)", "nom_region")

    val expected = spark.sparkContext.parallelize(
      List(
        (84, 3, "Auvergne-Rhône-Alpes"),
        (32, 21, "Hauts-de-France")
      )
    ).toDF("code_region", "avg_dep", "nom_region")

    When("renommage de la colonne avg(code_departement) en avg_dep")
    val actual = HelloWorld.renameColumn(input)

    Then("je dois avoir la colonne renommée en avg_dep")
    assertDataFrameEquals(actual, expected)

  }

  test("je veux vérifier que je lis un fichier, ajoute une colonne, la renomme, et sauvegarde mon fichier en parquet") {

    Given("une dataframe avec au moins 4 colonnes et un entête : code_departement, nom_departement, code_region et nom_region")

    val df = spark.sparkContext.parallelize(
      List(
        (1, "Ain", 84, "Auvergne-Rhône-Alpes"),
        (2, "Aisne", 32, "Hauts-de-France"),
        (3, "Allier", 84, "Auvergne-Rhône-Alpes")
      )
    ).toDF("code_departement", "nom_departement", "code_region", "nom_region")

    val exportPath = "src/test/resources/avg_output.parquet"

    val expected = Set("code_region", "avg_dep", "nom_region")

    When("je lance le traitement de mon fichier et je récupère le parquet")
    HelloWorld.startWithAvg(df, exportPath)
    val actual = spark.read.parquet(exportPath).columns.toSet

    Then("je dois avoir les 3 colonnes : code_region, avg_dep, nom_region")
    actual shouldEqual expected
  }

  test("je veux obtenir uniquement le nombre présent dans ma chaine de caractères") {

    Given("une chaine de caractères avec des chiffres et lettres")
    val input = "01aB2C"

    val expected = 12

    When("je convertis pour avoir mon nombre")
    val actual = HelloWorld.toInteger(input)

    Then("je dois avoir un nombre sans aucune lettre")
    actual shouldEqual expected

  }

  test("je veux enrichir mes données en calculant la moyenne et en modifiant le nom de la colonne") {

    Given("une dataframe avec au moins 3 colonnes : code région, numéro département et nom région")
    val input = spark.sparkContext.parallelize(
      List(
        (84, 2, "Auvergne-Rhône-Alpes"),
        (84, 3, "Auvergne-Rhône-Alpes"),
        (84, 4, "Auvergne-Rhône-Alpes"),
        (32, 14, "Hauts-de-France"),
        (32, 54, "Hauts-de-France"),
        (32, 7, "Hauts-de-France"),
        (32, 9, "Hauts-de-France")
      )
    ).toDF("code_region", "code_departement", "nom_region")

    val expected = spark.sparkContext.parallelize(
      List(
        (84, 3, "Auvergne-Rhône-Alpes"),
        (32, 21, "Hauts-de-France")
      )
    ).toDF("code_region", "avg_dep", "nom_region")

    When("lorsque j'enrichi mes données")
    val actual = HelloWorld.dataFrameEnrichment(input)

    Then("je dois avoir la moyenne des départements avec la colonne renommée en avg_dep")
    assertDataFrameEquals(actual, expected)
  }

  test("je veux faire la jointure de mes départements avec celui des villes") {

    Given("2 dataframes convertis pour departement : numéro département, nom région et pour les villes : id, department_code, name")
    val department = spark.sparkContext.parallelize(
      List(
        (1, "Auvergne-Rhône-Alpes"),
        (2, "Hauts-de-France")
      )
    ).toDF("code_departement", "nom_region")

    val city = spark.sparkContext.parallelize(
      List(
        ("1", 1, "L'Abergement-Clémenciat"),
        ("2", 1, "Ambérieu-en-Bugey"),
        ("3", 2, "Ambléon")
      )
    ).toDF("id", "department_code", "name")

    val expected = spark.sparkContext.parallelize(
      List(
        (1, "Auvergne-Rhône-Alpes", "1", 1, "L'Abergement-Clémenciat"),
        (1, "Auvergne-Rhône-Alpes", "2", 1, "Ambérieu-en-Bugey"),
        (2, "Hauts-de-France", "3", 2, "Ambléon")
      )
    ).toDF("code_departement", "nom_region", "id", "department_code", "name")

    When("lorsque je fais ma jointure de ces 2 dataframes")
    val actual = HelloWorld.joinDataFrame(department, city)

    Then("je dois avoir pour chacune des villes un département associé")
    assertDataFrameEquals(actual, expected)
  }

  test("je veux vérifier que je lis 2 fichiers, fasse une jointure et sauvegarde mon fichier en parquet") {

    Given("2 dataframes pour departement : code département, code_region, nom région et pour les villes : id, department_code, name")
    val department = spark.sparkContext.parallelize(
      List(
        ("1", "84", "Auvergne-Rhône-Alpes"),
        ("2", "32", "Hauts-de-France")
      )
    ).toDF("code_departement", "code_region", "nom_region")

    val city = spark.sparkContext.parallelize(
      List(
        ("1", "1", "L'Abergement-Clémenciat"),
        ("2", "1", "Ambérieu-en-Bugey"),
        ("3", "2", "Ambléon")
      )
    ).toDF("id", "department_code", "name")

    val expected = Set("nom_region", "id", "department_code", "name", "code_region", "code_departement")

    val exportPath = "src/test/resources/join_output.parquet"

    When("je lance le traitement de mon fichier et je récupère le parquet")
    HelloWorld.startWithJoin(department, city, exportPath)
    val actual = spark.read.parquet(exportPath).columns.toSet

    Then("je dois avoir les 6 colonnes dans mon fichier")
    actual shouldEqual expected
  }

}

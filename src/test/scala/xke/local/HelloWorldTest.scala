package xke.local

import org.scalatest.{FunSuite, GivenWhenThen}
import spark.{DataFrameAssertions, SharedSparkSession}

class HelloWorldTest extends FunSuite with GivenWhenThen with DataFrameAssertions {
  val spark = SharedSparkSession.sparkSession
  import spark.implicits._

  /*test("main must create a file with word count result") {
    Given("input filepath and output filepath")
    val input = "src/test/resources/input.txt"
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
  }*/
  test("je veux ajouter une colonne avec la moyenne des numéros département par région") {
    Given("une dataframe avec au moins 3 colonnes : nom région, code departement et nom region")
    val input = spark.sparkContext.parallelize(List(
      ("toto", 1, 2),
      ("titi", 1, 3),
      ("tata", 1, 4),
      ("zaza", 2, 14)
    )).toDF("nom_region", "code_region", "code_departement")
    val expected = spark.sparkContext.parallelize(List(
      (1, "toto", 2),
      (2, "zaza", 14),
      (1, "titi", 3),
      (1, "tata", 4)
    )).toDF("code_region", "nom_region", "code_departement")

    When("on calcule la moyenne des numéros de départements par région")
    val actual = HelloWorld.avgDepByReg(input)

    Then("Je dois retrouver les moyennes par régions des départements")
    assertDataFrameEquals(actual, expected)
  }

  test("je veux renommer la colonne des moyennes des numéros département") {

    Given("une dataframe avec au moins 3 colonnes : nom région, code région et numéro département")
    val input = spark.sparkContext.parallelize(List(
      ("toto", 1, 2.20),
      ("titi", 1, 3.33),
      ("tata", 1, 4.58),
      ("zaza", 2, 14.69)
    )).toDF("nom_region", "code_region", "avg(code_departement)")

    val expected = spark.sparkContext.parallelize(List(
      ("toto", 1, 2.20),
      ("titi", 1, 3.33),
      ("tata", 1, 4.58),
      ("zaza", 2, 14.69)
    )).toDF("nom_region", "code_region", "avg_dep")

    When("On renomme la dernière colonne ")
    val actual = HelloWorld.renameColumn(input)

    Then("Le nom de la 3e colonne doit être avg_dep")
    assertDataFrameEquals(actual, expected)
  }


  test("Sauvegarde mon fichier en parquet") {
    Given("Je lis mon parquet")
    HelloWorld.main(null)

    val actual = spark.read.parquet("src/main/ex1/parquet")
    When("Je compte le nombre de colonne de mon parquet")


    Then("Le nombre de colonne doit être 3")
    val expected = 3

    assert(actual.columns.length === expected)
  }


  test("Je récupère que les chiffres et je supprime les lettres") {

    Given("J'ai une chaine de caractère avec 2 chiffre: 1 et 5")
    val s = "1jgfmkk5"
    val expected = 15

    When("Je supprime les lettres")
    val actual = HelloWorld.parseInteger(s)

    Then("L'entier qui ressort doit être 15")
    assert(actual === expected)
  }


  test("Test de la jointure") {

    Given("Je possede deux dataFrames")
    val input1 = spark.sparkContext.parallelize(List(
      (1, 75,"bob"),
      (2,13,"boby"),
      ( 1, 92,"toto"),
      (2, 69,"dupond")
    )).toDF( "code_region", "code_departement","nom_region")


    val input2 = spark.sparkContext.parallelize(List(
      (75,"Paris"),
      (13,"Marseille"),
      (69,"Lyon"),
      (92,"Nanterre")
    )).toDF("department_code","nom_ville")

    When("J'effectue une jointure sur les 2 dataframes'")
    val actual = HelloWorld.joinDf(input1,input2)

    val expected = spark.sparkContext.parallelize(List(
      (2,13,"boby",13,"Marseille"),
      (1,92,"toto",92,"Nanterre"),
      (2, 69,"dupond" ,69,"Lyon"),
      ( 1, 75,"bob",75,"Paris")
    )).toDF("code_region","code_departement","nom_region","department_code" ,"nom_ville")
    Then("Je devrais avoir 5 colonne")

    assertDataFrameEquals(actual,expected)
  }

  test("Test de la création des partitions") {
    Given("Je crée ma partition")
    HelloWorld.main(null)

    When("Je compte le nombre de colonne de mon parquet")
    val actual = spark.read.parquet("src/main/parquet/code_region=04/code_departement=974/")


    Then("Le nombre de colonne doit être 3")
    val expected = 10

    assert(actual.columns.length === expected)
  }

}

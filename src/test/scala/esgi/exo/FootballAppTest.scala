package esgi.exo

import esgi.exo.FootballApp.isDomicile
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.scalatest.{FunSuite, GivenWhenThen}
import spark.{DataFrameAssertions, SharedSparkSession}

class FootballAppTest extends FunSuite with GivenWhenThen with DataFrameAssertions {
  val spark = SharedSparkSession.sparkSession
  import spark.implicits._

  test("je veux renommer la colonnes X4 en match") {
    Given("une dataframe avec au moins 13 colonnes : X2, X4, X5, X6, adversaire, score_france, score_adversaire, penalty_france, penalty_adversaire, date, year, outcome, no")
    val input = spark.sparkContext.parallelize(
      List(
        ("1er mai 1904", "Belgique - France", "3-3", "Match amical", "Belgique", "3", "3", "NA", "NA", "1904-05-01", "1904", "draw", "1")
      ))
      .toDF(
        "X2", "X4", "X5", "X6", "adversaire", "score_france", "score_adversaire", "penalty_france", "penalty_adversaire", "date", "year", "outcome",  "no")

    val expected = spark.sparkContext.parallelize(
      List(
        ("1er mai 1904", "Belgique - France", "3-3", "Match amical", "Belgique", "3", "3", "NA", "NA", "1904-05-01", "1904", "draw", "1")
      ))
      .toDF(
        "X2", "match", "X5", "X6", "adversaire", "score_france", "score_adversaire", "penalty_france", "penalty_adversaire", "date", "year", "outcome",  "no")

    When("Je renomme")
    val actual = FootballApp.renameColumn(input,"X4", "match")

    Then("Je m'attend à ce que la colonne soit renommé")
    assertDataFrameEquals(actual, expected)
  }

  test("je veux selectionner certaines colonnes pour supprimer les colonnes  X2, X5, year, outcome") {
    Given("une dataframe avec au moins 13 colonnes : X2, match, X5, competition, adversaire, score_france, score_adversaire, penalty_france, penalty_adversaire, date, year, outcome, no")
    val input = spark.sparkContext.parallelize(
      List(
        ("1er mai 1904", "Belgique - France", "3-3", "Match amical", "Belgique", "3", "3", "NA", "NA", "1904-05-01", "1904", "draw", "1")
      ))
      .toDF(
        "X2", "match", "X5", "competition", "adversaire", "score_france", "score_adversaire", "penalty_france", "penalty_adversaire", "date", "year", "outcome",  "no")

    val expected = spark.sparkContext.parallelize(
      List(
        ("Belgique - France", "Match amical", "Belgique", "3", "3", "NA", "NA", "1904-05-01")
      ))
      .toDF(
        "match", "competition", "adversaire", "score_france", "score_adversaire", "penalty_france", "penalty_adversaire", "date")

    When("Je sélectionne")
    val actual = FootballApp.selectColumns(input)

    Then("Je m'attend à ce que certaines colonnes soient supprimées")
    assertDataFrameEquals(actual, expected)
  }

  test("je veux selectionner remplacer les valeurs null par 0") {
    Given("une dataframe avec au moins 8 colonnes : match, competition, adversaire, score_france, score_adversaire, penalty_france, penalty_adversaire")
    val input = spark.sparkContext.parallelize(
      List(
        ("Belgique - France", "Match amical", "Belgique", "3", "3", "0", "NA", "1904-05-01")
      ))
      .toDF(
        "match", "competition", "adversaire", "score_france", "score_adversaire", "penalty_france", "penalty_adversaire", "date")

    val expected = spark.sparkContext.parallelize(
      List(
        ("Belgique - France", "Match amical", "Belgique", "3", "3", "0", "0", "1904-05-01")
      ))
      .toDF(
        "match", "competition", "adversaire", "score_france", "score_adversaire", "penalty_france", "penalty_adversaire", "date")

    When("Je retire les éléments null")
    val actual = FootballApp.removeNull(input)

    Then("Je m'attend à ce qu'il n'y ai pas d'éléments nul dans les pénalties")
    assertDataFrameEquals(actual, expected)
  }

  test("je veux filter par date supérieur à 1980-03-01") {
    Given("une dataframe avec au moins 8 colonnes : match, competition, adversaire, score_france, score_adversaire, penalty_france, penalty_adversaire")
    val input = spark.sparkContext.parallelize(
      List(
        ("Belgique - France", "Match amical", "Belgique", "3", "3", "0", "0", "1980-03-02"),
          ("Belgique - France", "Match amical", "Belgique", "3", "3", "0", "0", "1978-03-01")
      ))
      .toDF(
        "match", "competition", "adversaire", "score_france", "score_adversaire", "penalty_france", "penalty_adversaire", "date")

    val expected = spark.sparkContext.parallelize(
      List(
        ("Belgique - France", "Match amical", "Belgique", "3", "3", "0", "0", "1980-03-02")
      ))
      .toDF(
        "match", "competition", "adversaire", "score_france", "score_adversaire", "penalty_france", "penalty_adversaire", "date")

    When("Je filtre")
    val actual = FootballApp.filterMatchGt1980(input)

    Then("Je m'attend à ce qu'il n'y ai des dates supérieur à 1980-03-01")
    assertDataFrameEquals(actual, expected)
  }

  test("je veux écrire dans mon parquet les stats") {
    Given("une dataframe avec au moins 8 colonnes : match, competition, adversaire, score_france, score_adversaire, penalty_france, penalty_adversaire")
    val input = spark.sparkContext.parallelize(
      List(
        ("Belgique - France", "Match amical", "Belgique", "3", "3", "0", "0", "1980-03-02"),
        ("Belgique - France", "Match amical", "Belgique", "3", "3", "0", "0", "1978-03-01")
      ))
      .toDF(
        "match", "competition", "adversaire", "score_france", "score_adversaire", "penalty_france", "penalty_adversaire", "date")

    When("J'écris")
    FootballApp.writeStats(input)

    Then("Je m'attend à avoirs 2 résultats")
    spark.read.parquet("src/main/resources/stats.parquet").count() shouldEqual  2
  }

  test("je veux écrire dans mon parquet les resultats des matches") {
    Given("une dataframe avec au moins 8 colonnes : match, competition, adversaire, score_france, score_adversaire, penalty_france, penalty_adversaire")
    val input = spark.sparkContext.parallelize(
      List(
        ("Belgique - France", "Match amical", "Belgique", "3", "3", "0", "0", "1980-03-02"),
        ("Belgique - France", "Match amical", "Belgique", "3", "3", "0", "0", "1978-03-01")
      ))
      .toDF(
        "match", "competition", "adversaire", "score_france", "score_adversaire", "penalty_france", "penalty_adversaire", "date")

    When("J'écris")
    FootballApp.writeResult(input)

    Then("Je m'attend à avoirs 2 résultats")
    spark.read.parquet("src/main/resources/result.parquet").count() shouldEqual  2
  }

  test("je veux filter les lignes dont les colonnes ne sont pas valides") {
    Given("une dataframe avec au moins 8 colonnes : match, competition, adversaire, score_france, score_adversaire, penalty_france, penalty_adversaire")
    val input = spark.sparkContext.parallelize(
      List(
        ("Belgique - France", "Match amical", "Belgique", "3", "3", "0", "0", "1980-03-02"),
        (" Croates et Slovènes\"", "Match amical", "Belgique", "3", "3", "0", "0", "1978-03-01")
      ))
      .toDF(
        "match", "competition", "adversaire", "score_france", "score_adversaire", "penalty_france", "penalty_adversaire", "date")

    val expected = spark.sparkContext.parallelize(
      List(
        ("Belgique - France", "Match amical", "Belgique", "3", "3", "0", "0", "1980-03-02")
      ))
      .toDF(
        "match", "competition", "adversaire", "score_france", "score_adversaire", "penalty_france", "penalty_adversaire", "date")

    When("Je filtre")
    val actual = FootballApp.clean_data(input)

    Then("Je m'attend à ce que les lignes soient filtrés")
    assertDataFrameEquals(actual, expected)
  }

  test("je veux ajouter une colonne joue") {
    Given("une dataframe avec au moins 8 colonnes : match, competition, adversaire, score_france, score_adversaire, penalty_france, penalty_adversaire")
    val input = spark.sparkContext.parallelize(
      List(
        ("Belgique - France", "Match amical", "Belgique", "3", "3", "0", "0", "1980-03-02")
      ))
      .toDF(
        "match", "competition", "adversaire", "score_france", "score_adversaire", "penalty_france", "penalty_adversaire", "date")

    val expected = spark.sparkContext.parallelize(
      List(
        ("Belgique - France", "Match amical", "Belgique", "3", "3", "0", "0", "1980-03-02", false)
      ))
      .toDF(
        "match", "competition", "adversaire", "score_france", "score_adversaire", "penalty_france", "penalty_adversaire", "date", "joue")

    When("J'ajoute la colonne")
    val actual = FootballApp.addColumnJoue(input)

    Then("Je m'attend à ce que la colonne soit ajoutée")
    assertDataFrameEquals(actual, expected)
  }

//  test("je veux effectuer les stats des matches") {
//    Given("une dataframe avec au moins 8 colonnes : match, competition, adversaire, score_france, score_adversaire, penalty_france, penalty_adversaire")
//    val input = spark.sparkContext.parallelize(
//      List(
//        ("Belgique - France", "Match amical", "Belgique", "3", "3", "0", "0", "1980-03-02")
//      ))
//      .toDF(
//        "match", "competition", "adversaire", "score_france", "score_adversaire", "penalty_france", "penalty_adversaire", "date")
//
//    val expected = spark.sparkContext.parallelize(
//      List(
//        ("Belgique - France", "Match amical", "Belgique", "3", "3", "0", "0", "1980-03-02", false)
//      ))
//      .toDF(
//        "adversaire", "competition", "adversaire", "score_france", "score_adversaire", "penalty_france", "penalty_adversaire", "date", "joue")
//
//    When("J'ajoute la colonne")
//    val actual = FootballApp.addColumnJoue(input)
//
//    Then("Je m'attend à ce que la colonne soit ajoutée")
//    assertDataFrameEquals(actual, expected)
//  }

}
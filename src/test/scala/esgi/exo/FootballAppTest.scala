package esgi.exo

import esgi.exo.FootballApp.mat
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

//  test("je veux selectionner remplacer les valeurs null par 0") {
//    Given("une dataframe avec au moins 8 colonnes : match, competition, adversaire, score_france, score_adversaire, penalty_france, penalty_adversaire")
//    val input = spark.sparkContext.parallelize(
//      List(
//        ("Belgique - France", "Match amical", "Belgique", "3", "3", "0", null, "1904-05-01")
//      ))
//      .toDF(
//        "match", "competition", "adversaire", "score_france", "score_adversaire", "penalty_france", "penalty_adversaire", "date")
//
//    val expected = spark.sparkContext.parallelize(
//      List(
//        ("Belgique - France", "Match amical", "Belgique", "3", "3", "0", "0", "1904-05-01")
//      ))
//      .toDF(
//        "match", "competition", "adversaire", "score_france", "score_adversaire", "penalty_france", "penalty_adversaire", "date")
//
//    When("Je retire les éléments null")
//    val actual = FootballApp.removeNull(input)
//
//    Then("Je m'attend à ce qu'il n'y ai pas d'éléments nul dans les pénalties")
//    assertDataFrameEquals(actual, expected)
//  }

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

//  test("je veux ajouter une colonne pour savoir si le match a été joué à domicile") {
//    Given("une dataframe avec au moins 8 colonnes : match, competition, adversaire, score_france, score_adversaire, penalty_france, penalty_adversaire")
//
//    val splitColumn: UserDefinedFunction = udf(mat _)
//
//    val input = spark.sparkContext.parallelize(
//      List(
//        ("Belgique - France", "Match amical", "Belgique", "3", "3", "0", "0", "1980-03-02")
//      ))
//      .toDF(
//        "match", "competition", "adversaire", "score_france", "score_adversaire", "penalty_france", "penalty_adversaire", "date")
//
//    val expected = spark.sparkContext.parallelize(
//      List(
//        ("Belgique - France", "Match amical", "Belgique", "3", "3", "0", "0", "1980-03-02")
//      ))
//      .toDF(
//        "match", "competition", "adversaire", "score_france", "score_adversaire", "penalty_france", "penalty_adversaire", "date", "joue")
//
//    When("J'a")
//    val actual = FootballApp.mat(input)
//
//    Then("Je m'attend à ce qu'il n'y ai une dataframe avec une nouvelle colonne joue")
//    assertDataFrameEquals(actual, expected)
//  }


}
package esgi.exo

import org.scalatest.{FunSuite, GivenWhenThen}
import spark.{DataFrameAssertions, SharedSparkSession}

class FootballAppTest extends FunSuite with GivenWhenThen with DataFrameAssertions {
  val spark = SharedSparkSession.sparkSession
  import spark.implicits._

  test("Je veux enlever un 'NA'") {
    Given("Ma string 'NA' ce que je souhaite en sortie")
    val input = "NA"
    val expected = "0"

    When("J'applique la fonction")
    val actual = FootballApp.replaceNABy0(input)

    Then("Je devrais avoir '0'")
    assert(actual == expected)
  }

  test("Je veux enlever un 'NA', mais il n'y en a pas") {
    Given("Ma string à tester et ce que je souhaite en sortie")
    val input = "4"
    val expected = "4"

    When("J'applique la fonction")
    val actual = FootballApp.replaceNABy0(input)

    Then("L'input devrait rester inchangé")
    assert(actual == expected)
  }

  test("Je veux renommer des colonnes") {
    Given("Mon dataframe, et ce que je veux en sortie")
    val input = List(
      ("12 mai 1964", "France - Belgique", "Match amical"),
      ("24 octobre 1984", "Allemagne - France", "Match amical")
    ).toDF("X2", "X4", "X6")
    val expected = List(
      ("12 mai 1964", "France - Belgique", "Match amical"),
      ("24 octobre 1984", "Allemagne - France", "Match amical")
    ).toDF("X2", "match", "competition")

    When("Je renomme les colonnes")
    val actual = FootballApp.renameColumns(input)

    Then("Les colonnes devraient être renommées")
    assertDataFrameEquals(actual, expected)
  }

  test("Je veux sélectionner seulement certaines colonnes") {
    Given("Mon dataframe, et ce que je veux en sortie")
    val input = List(
      ("France - Belgique", "2-0", "Match amical", "Belgique", "2", "0", "NA", "NA", "1947-03-12", "1947", "1"),
      ("Allemagne - France", "1-1", "Match amical", "Allemagne", "1", "1", "NA", "1", "1946-12-01", "1946", "2")
    ).toDF(
      "match",
      "X5",
      "competition",
      "adversaire",
      "score_france",
      "score_adversaire",
      "penalty_france",
      "penalty_adversaire",
      "date",
      "year",
      "no"
    )
    val expected = List(
      ("France - Belgique", "Match amical", "Belgique", "2", "0", "NA", "NA", "1947-03-12", "1947"),
      ("Allemagne - France", "Match amical", "Allemagne", "1", "1", "NA", "1", "1946-12-01", "1946")
    ).toDF(
      "match",
      "competition",
      "adversaire",
      "score_france",
      "score_adversaire",
      "penalty_france",
      "penalty_adversaire",
      "date",
      "year"
    )

    When("Je sélectionne les colonnes")
    val actual = FootballApp.selectColumns(input)

    Then("Je devrais avoir les colonnes que je veux")
    assertDataFrameEquals(actual, expected)
  }

  test("Je veux remplacer les 'NA' par des '0'") {
    Given("Mon dataframe, et ce que je veux en sortie")
    Given("Mon dataframe, et ce que je veux en sortie")
    val input = List(
      ("France - Belgique", "Match amical", "NA", "NA"),
      ("Allemagne - France", "Match amical", "2", "1"),
      ("France - Colombie", "JO", "NA", "1"),
      ("France - Brésil", "JO", "2", "NA")
    ).toDF( "match", "competition", "penalty_france", "penalty_adversaire")
    val expected = List(
      ("France - Belgique", "Match amical", "0", "0"),
      ("Allemagne - France", "Match amical", "2", "1"),
      ("France - Colombie", "JO", "0", "1"),
      ("France - Brésil", "JO", "2", "0")
    ).toDF("match", "competition", "penalty_france", "penalty_adversaire")

    When("Je renomme les colonnes")
    val actual = FootballApp.replaceNullValues(input)

    Then("Les colonnes devraient être renommées")
    assertDataFrameEquals(actual, expected)
  }

  test("Je ne veux garder que les matchs que depuis 1980") {
    Given("Mon dataframe, et ce que je veux en sortie")
    val input = List(
      ("France - Belgique", "1992"),
      ("Allemagne - France", "1903"),
      ("France - Colombie", "1980"),
      ("France - Brésil", "1982"),
      ("France - Allemagne", "1942")
    ).toDF( "match", "year")
    val expected = List(
      ("France - Belgique", "1992"),
      ("France - Colombie", "1980"),
      ("France - Brésil", "1982")
    ).toDF( "match", "year")

    When("Je filtre sur l'année")
    val actual = FootballApp.keepOnlySince1980(input)

    Then("Je ne devrais plus avoir les matchs d'avant 1980")
    assertDataFrameEquals(actual, expected)
  }

  test("Je veux ajouter une colonne indiquant si le match a été joué à domicile") {
    Given("Mon dataframe, et ce que je veux en sortie")
    val input = List(
      ("France - Belgique", "1992"),
      ("Allemagne - France", "1903"),
      ("France - Colombie", "1980"),
      ("Chine - France", "1980")
    ).toDF( "match", "year")
    val expected = List(
      ("France - Belgique", "1992", true),
      ("Allemagne - France", "1903", false),
      ("France - Colombie", "1980", true),
      ("Chine - France", "1980", false)
    ).toDF( "match", "year", "a_domicile")

    When("J'ajoute la colonne")
    val actual = FootballApp.addColumnAtHome(input)

    Then("Je devrais avoir la nouvelle colonne")
    assertDataFrameEquals(actual, expected)
  }

  test("Je veux calculer les statistiques") {
    Given("Mon dataframe, et ce que je veux en sortie")
    val input = List(
      ("France - Belgique", "Coupe du monde 2006", "Belgique", "2", "1", "0", "0", true),
      ("France - Belgique", "match amical", "Belgique", "3", "2", "0", "1", true),
      ("Belgique - France", "match amical", "Belgique", "1", "0", "0", "0", false),
      ("Allemagne - France", "Coupe du monde 1998", "Allemagne", "2", "1", "2", "1", false),
      ("Allemagne - France", "match amical", "Allemagne", "2", "1", "0", "0", false),
      ("France - Allemagne", "Coupe du monde 20012", "Allemagne", "1", "2", "2", "0", true),
      ("France - Allemagne", "match amical", "Allemagne", "3", "2", "0", "2", true),
      ("France - Colombie", "match amical", "Colombie", "3", "3", "0", "0", true),
      ("France - Colombie", "match amical", "Colombie", "3", "1", "0", "0", true)
    ).toDF(
      "match",
      "competition",
      "adversaire",
      "score_france",
      "score_adversaire",
      "penalty_france",
      "penalty_adversaire",
      "a_domicile"
    )
    val expected = List(
      ("Belgique", "2.0", "1.0", "3", "1", "66.66666666666667", "0", "-1.0"),
      ("Allemagne", "2.0", "1.5", "4", "2", "50.0", "2", "1.0"),
      ("Colombie", "3.0", "2.0", "2", "0", "100.0", "0", "0.0")
    ).toDF(
      "adversaire",
      "avg_score_france",
      "avg_score_adversaire",
      "nb_matchs",
      "nb_matchs_CDM",
      "pourcentage_a_domicile",
      "max_penalty_france",
      "indice_penalty"
    )

    When("Je calcule les stats")
    val actual = FootballApp.calculateStats(input)

    Then("Je devrais avoir les bonnes stats")
    assertDataFrameEquals(actual, expected)
  }

  test("Je veux faire la jointure entre le dataframe 1 et les stats") {
    Given("Les deux dataframes, et ce que je veux en sortie")
    val inputDataframe1 = List(
      ("France - Belgique", "Belgique", "2", "1", "0", "0", true),
      ("France - Belgique", "Belgique", "4", "5", "1", "0", true),
      ("France - Allemagne", "Allemagne", "3", "0", "0", "1", true),
      ("France - Allemagne", "Allemagne", "3", "2", "0", "2", true)
    ).toDF(
      "match",
      "adversaire",
      "score_france",
      "score_adversaire",
      "penalty_france",
      "penalty_adversaire",
      "a_domicile"
    )
    val inputStats = List(
      ("Belgique", "3.0", "3.0", "2", "100.0", "1", "1.0"),
      ("Allemagne", "6.0", "1.0", "2", "100.0", "0", "-3.0")
    ).toDF(
      "adversaireBis",
      "avg_score_france",
      "avg_score_adversaire",
      "nb_matchs",
      "pourcentage_a_domicile",
      "max_penalty_france",
      "indice_penalty"
    )
    val expected = List(
      ("France - Belgique", "Belgique", "2", "1", "0", "0", true, "3.0", "3.0", "2", "100.0", "1", "1.0"),
      ("France - Belgique", "Belgique", "4", "5", "1", "0", true, "3.0", "3.0", "2", "100.0", "1", "1.0"),
      ("France - Allemagne", "Allemagne", "3", "0", "0", "1", true, "6.0", "1.0", "2", "100.0", "0", "-3.0"),
      ("France - Allemagne", "Allemagne", "3", "2", "0", "2", true, "6.0", "1.0", "2", "100.0", "0", "-3.0")
    ).toDF(
      "match",
      "adversaire",
      "score_france",
      "score_adversaire",
      "penalty_france",
      "penalty_adversaire",
      "a_domicile",
      "avg_score_france",
      "avg_score_adversaire",
      "nb_matchs",
      "pourcentage_a_domicile",
      "max_penalty_france",
      "indice_penalty"
    )

    When("Je fais la jointure")
    val actual = FootballApp.joinDFBaseAndStats(inputDataframe1, inputStats)

    Then("Je devrais avoir la bonne jointure")
    assertDataFrameEquals(actual, expected)
  }

  test("Je veux ajouter une colonne indiquant le mois") {
    Given("Mon dataframe avec la date, et ce que je veux en sortie")
    val input = List(
      ("France - Belgique", "1992-05-03"),
      ("Allemagne - France", "1903-11-14")
    ).toDF( "match", "date")
    val expected = List(
      ("France - Belgique", "1992-05-03", "05"),
      ("Allemagne - France", "1903-11-14", "11")
    ).toDF( "match", "date", "month")

    When("J'ajoute la colonne")
    val actual = FootballApp.addColumnMonth(input)

    Then("Je devrais avoir la nouvelle colonne")
    assertDataFrameEquals(actual, expected)
  }
}

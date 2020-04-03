package esgi.exo

import org.scalatest.{FunSuite, GivenWhenThen}
import spark.{DataFrameAssertions, SharedSparkSession}

class FootballAppTest extends FunSuite with GivenWhenThen with DataFrameAssertions {
  val spark = SharedSparkSession.sparkSession
  import spark.implicits._


  //Partie 1 : Nettoyage des données

  test("x4 doit devenir match"){
      Given("Un dataframe avec une colonne nommée x4")
      val input = List(
        ("1",1),
        ("2",2)
      ).toDF(
        "x4","x5"
      )
      val toChange = "x4"
      val changeTo = "match"

      val expected = List(
        ("1",1),
        ("2",2)
      ).toDF(
        "match","x5"
      )

      When("Je renomme")
      val actual = FootballApp.rename(input, toChange, changeTo)

      When("'x4' devient 'match' ")
    assertDataFrameEquals(actual, expected)
  }

  test("x6 doit devenir competition"){
    Given("Un dataframe avec une colonne nommée x6")
    val input = List(
      ("1",1),
      ("2",2)
    ).toDF(
      "x1","x6"
    )
    val toChange = "x6"
    val changeTo = "competition"

    val expected = List(
      ("1",1),
      ("2",2)
    ).toDF(
      "x1","competition"
    )

    When("Je renomme")
    val actual = FootballApp.rename(input, toChange, changeTo)

    When("'x4' devient 'match' ")
    assertDataFrameEquals(actual, expected)
  }

  test("Je dois avoir les bonnes colonnes"){
    Given("J'ai un dataframe comptenant 10 colonnes")
    val input = List(
      (1,2,3,4,5,6,7,8,9,10)
    ).toDF(
      "match",
      "competition",
      "adversaire",
      "score_france",
      "entrop1",
      "score_adversaire",
      "penalty_france",
      "penalty_adversaire",
      "date",
      "entrop2"
    )
    val expected = List(
      (1,2,3,4,6,7,8,9)
    ).toDF(
      "match",
      "competition",
      "adversaire",
      "score_france",
      "score_adversaire",
      "penalty_france",
      "penalty_adversaire",
      "date"
    )
    When("Je filtre")
    val actual = FootballApp.filterColumns(input)

    Then("seul les 8 colonnes voulues restent")

    assertDataFrameEquals(actual, expected)

  }


  test("Mes nuls doivent être remplacées par des 0 dans un dataframe"){
    Given("Un dataset avec des colonnes penalty")

    val input= List(
      ("1","NA","1"),
      ("2","2","NA"),
      ("3","3","3"),
      ("4","NA","NA")
    ).toDF(
      "num","penalty_france", "penalty_adversaire"
    )

    val expected= List(
      (1,0,1),
      (2,2,0),
      (3,3,3),
      (4,0,0)
    ).toDF(
      "num","penalty_france", "penalty_adversaire"
    )

    When("Je remplace les valeurs nulle")
    val actual = FootballApp.removeNullsToPenalties(input)

    Then("Mes nulls sont à 0")
    assertDataFrameEquals(expected,actual)
  }

  test("Je dois remplacer une valeur nulle unique par un 0"){
    Given("Une valeure non renseignée")
    val input = "NA"
    val expected = 0
    When("Je la controle")
    val actual = FootballApp.noDataTo0(input)
    Then("Elle doit être 0")
    assert(expected == actual)
  }

  test("Je ne dois pas remplacer une valeur non nulle unique par un 0"){
    Given("Une valeure renseignée")
    val input = "974"
    val expected = 974
    When("Je la controle")
    val actual = FootballApp.noDataTo0(input)
    Then("Elle doit être 974")
    assert(expected == actual)
  }

  test("Je ne dois pas avoir de match datant d'avant mars 1980"){
    Given("un datasource avec des dates")
    val input = List(
      (1,"1925-01-01"),
      (2,"1950-02-02"),
      (3,"1980-03-03"),
      (4,"2000-04-04")
    ).toDF(
      "value", "date"
    )
    val expected = List(
      (3,"1980-03-03"),
      (4,"2000-04-04")
    ).toDF(
      "value", "date"
    )

    When("Je selectionne les données à partire de mars 1980")

    val actual = FootballApp.filterByDate(input, "1980-03-01")

    Then("Il ne me reste que 2 lignes")
    assertDataFrameEquals(actual,expected)
  }

  // Exo2 : Calcul de statistiques

  test("Match à domicile"){

  }

  test("Match en extérieur"){

  }

  test("nb points marqué par la Fr par match"){

  }

  test("nb points encaissé par la Fr par match"){

  }

  test("nb points match total"){

  }

  test("Pourcentage de match à domicile"){

  }

  test("nombre de match joué en coupe du monde"){

  }

  test("plus grand nombre de pénalité reçu par la france dans un match"){

  }

  test("  nombre de pénalité total reçu par la France moins nombre de pénalité total reçu par l’adversaire"){

  }


}
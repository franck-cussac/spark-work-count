package esgi.exo

import org.scalatest.{FunSuite, GivenWhenThen}
import spark.{DataFrameAssertions, SharedSparkSession}

class FootballAppTest extends FunSuite with GivenWhenThen with DataFrameAssertions {
  val spark = SharedSparkSession.sparkSession
  import spark.implicits._


  test("je veux renommer les colonnes X4 et X6 en match et competition") {
    Given("une dataframe avec au les colonnes X2, X4, X6... et des matchs anterieurs à 1980")
    val input = spark.sparkContext.parallelize(
      List(
        ("1er mai 1904","Belgique - France","3-3","Match amical","Belgique","","","","",""),
        ("12 février 1905","France - Suisse","1-0","Match amical","Suisse","","","","",""),
        ("26 juin 2018","Danemark - France","0-0","Coupe du monde 2018 (Groupe C)","Danemark","","","","",""),
        ("21 juin 2018","France - Pérou","1-0","Coupe du monde 2018 (Groupe C)","Pérou","","","","","")
      )
    ).toDF("X2",
      "X4",
      "X5",
      "X6",
      "adversaire",
      "score_france",
      "score_adversaire",
      "penalty_france" ,
      "penalty_adversaire",
      "date")

    When("On renomme les colonnes X4 et X6 et clean les colonnes penalty")

    val expected = spark.sparkContext.parallelize(
      List(
        ("Belgique - France","Match amical","Belgique","","",0,0,""),
        ("France - Suisse","Match amical","Suisse","","",0,0,""),
        ("Danemark - France","Coupe du monde 2018 (Groupe C)","Danemark","","",0,0,""),
        ("France - Pérou","Coupe du monde 2018 (Groupe C)","Pérou","","",0,0,"")
      )
    ).toDF("match",
      "competition",
      "adversaire",
      "score_france",
      "score_adversaire",
      "penalty_france" ,
      "penalty_adversaire",
      "date")


    val actual = FootballApp.selectAndRenameColumns(input)

    Then("On devrait obtenir expected")
    assertDataFrameEquals(actual, expected)
  }


  test("je veux uniquement les matchs après 1980") {
    Given("une dataframe avec au moins les colonnes year et match")
    val input = spark.sparkContext.parallelize(
      List(
        ("1904","Belgique - France"),
        ("1905","France - Suisse"),
        ("2018","Danemark - France"),
        ("2018","France - Pérou")
      )
    ).toDF("year", "match")

    When("On selectionne uniquement les matchs après 1980")

    val expected = spark.sparkContext.parallelize(
      List(
        ("2018","Danemark - France"),
        ("2018","France - Pérou")
      )
    ).toDF("year", "match")


    val actual = FootballApp.filterDate(input)

    Then("On devrait obtenir expected")
    assertDataFrameEquals(actual, expected)
  }


  test("je veux faire des stats sur les matchs par adversaire") {
    Given("une dataframe avec au moins les colonnes adversaire score_france score_adversaire match at_home competition penalty_france et penalty_adversaire")
    val input = spark.sparkContext.parallelize(
      List(
        ("Belgique", 2, 0, "France - Belgique", true, "Compet1", 0, 0),
        ("Belgique", 2, 4, "Belgique - France", false, "Coupe du monde 2018 (Groupe C)", 1, 2),
        ("Hongrie", 4, 0, "Hongrie - France", false, "Compet2", 1, 0)
      )
    ).toDF("adversaire",
      "score_france",
      "score_adversaire",
      "match",
      "at_home",
      "competition",
      "penalty_france",
      "penalty_adversaire")

    When("On regroupe par adversaire et fesons des stats")

    val expected = spark.sparkContext.parallelize(
      List(
        ("Belgique", 2, 2, 2, 1, 50, -1),
        ("Hongrie", 4, 0, 1, 0, 0, 1)
      )
    ).toDF("adversaire",
      "average_goals_france",
      "average_goals_opponent",
      "number_of_matches",
      "number_of_WC_matches",
      "home_percentage",
      "diff_penalty")

    val actual = FootballApp.exo2(input)

    Then("On devrait obtenir expected")
    assertDataFrameEquals(actual, expected)
  }





}

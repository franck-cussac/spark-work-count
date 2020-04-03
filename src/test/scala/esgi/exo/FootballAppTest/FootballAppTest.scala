package esgi.exo.FootballAppTest

import esgi.exo.FootballApp.FootballApp
import org.scalatest.{FunSuite, GivenWhenThen}
import spark.{DataFrameAssertions, SharedSparkSession}

class FootballAppTest  extends FunSuite with GivenWhenThen with DataFrameAssertions {
  val spark = SharedSparkSession.sparkSession
  import spark.implicits._

  test("je veux selectionner certaines colonnes") {
    Given("Je crée ma dataframe")
   val input = spark.sparkContext.parallelize(List(
     ("1er mai 1904","Belgique - France", "3-3","Match amical","Belgique","3","3","NA","NA","1904-05-01","1904","draw","1"),
     ("2 mars 2007","Allemagne - France", "0-3","Match amical","Allemagne","3","3","NA","NA","1904-05-01","1904",  "draw", "1"),
     ("15 juin mai 2014","France-Algerie", "1-0","Coupe du monde","France","0","1","NA","NA","2014-06-15","2014",  "draw", "3")
   )).toDF("X2", "X4","X5","X6","adversaire","score_france","score_adversaire","penalty_france","penalty_adversaire","date","year","outcome","no")

    When("Je fait appel à ma fonction renameAndSelect")
    val actual = FootballApp.renameAndSelect(input)
    val expected = 8
    Then("Je dois avoir 8 colonnes")
    assert(actual.columns.length === expected)
  }

  test("je ne veux plus de valeur null") {
    Given("Je crée ma dataframe")
    val input = spark.sparkContext.parallelize(List(
      ("1er mai 1904","Belgique - France", "3-3","Match amical","Belgique","3","3","NA","NA","1904-05-01","1904","draw","1"),
      ("2 mars 2007","Allemagne - France", "0-3","Match amical","Allemagne","3","3","NA","NA","1904-05-01","1904",  "draw", "1"),
      ("2 mars 2007","Allemagne - France", "0-3","Match amical","Allemagne","3","3","NA","NA","1904-05-01","1904",  "draw", "1")

    )).toDF("X2", "X4","X5","X6","adversaire","score_france","score_adversaire","penalty_france","penalty_adversaire","date","year","outcome","no")

    When("Je fait appel à ma fonction renameAndSelect")
    val actual = FootballApp.deleteNullValue(input)

    val expected = spark.sparkContext.parallelize(List(
      ("1er mai 1904","Belgique - France", "3-3","Match amical","Belgique","3","3","0","0","1904-05-01","1904","draw","1"),
      ("2 mars 2007","Allemagne - France", "0-3","Match amical","Allemagne","3","3","0","0","1904-05-01","1904",  "draw", "1"),
      ("2 mars 2007","Allemagne - France", "0-3","Match amical","Allemagne","3","3","0","0","1904-05-01","1904",  "draw", "1")

    )).toDF("X2", "X4","X5","X6","adversaire","score_france","score_adversaire","penalty_france","penalty_adversaire","date","year","outcome","no")

    Then("Je dois avoir les colonnes match,competition,adversaire,score_france,score_adversaire,penalty_france,penalty_adversaire,date")
    assertDataFrameEquals(actual,expected)

  }



}

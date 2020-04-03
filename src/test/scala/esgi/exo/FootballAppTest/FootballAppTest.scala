package esgi.exo

import org.scalatest.{FunSuite, GivenWhenThen}
import spark.{DataFrameAssertions, SharedSparkSession}

class FootballAppTest extends FunSuite with GivenWhenThen with DataFrameAssertions{
  val spark = SharedSparkSession.sparkSession
  import spark.implicits._

  // 1
  test("Clean dataframe") {
    Given("Dataframe not cleaned")
    val base = spark.sparkContext.parallelize(
      List(
        ("France - Allemagne", "Match amical", "Allemagne", "6", "4", "NA", "0", "1980-01-02"),
        ("France - Belgique", "Match amical", "Belgique", "6", "4", "2", "NA", "1979-01-02")
      )
    ).toDF("X4", "X6", "adversaire", "score_france", "score_adversaire", "penalty_france", "penalty_adversaire", "date")

    When("I call cleanData")
    val cleanBase = FootballApp.cleanData(base)

    Then("Dataframe is cleaned")
    val expected = spark.sparkContext.parallelize(
      List(
        ("France - Allemagne", "Match amical", "Allemagne", "6", "4", "0", "0", "1980-01-02")
      )
    ).toDF("match", "competition", "adversaire", "score_france", "score_adversaire", "penalty_france", "penalty_adversaire", "date")

    assertDataFrameEquals(cleanBase, expected)
  }

  // 2

//  Faire test udf

  test("Check isMatchAtHome returns true") {
    Given("a string descripting a match")
    val m = "France - Belgique"

    When("I call the isMatchAtHome function")
    val res = FootballApp.isMatchAtHome(m)

    Then("Match is at home")

    assert(res === true)
  }

  test("Check isMatchAtHome returns false") {
    Given("a string descripting a match")
    val m = "Belgique - France"

    When("I call the isMatchAtHome function")
    val res = FootballApp.isMatchAtHome(m)

    Then("Match isn't home")
    assert(res === false)
  }

  test("Calculate avg of france's goals") {
    Given("DataFrame with french goals pert match")
    val base = spark.sparkContext.parallelize(
      List(
        ("Andorre", "3"),
        ("Andorre", "1"),
        ("Andorre", "2"),
        ("Belgique", "5"),
        ("Belgique", "0")
      )
    ).toDF("adversaire", "score_france")

    When("I call avgGoalFrance")
    val avg = FootballApp.avgGoalFrance(base)

    Then("I get the correct average")
    val expected = spark.sparkContext.parallelize(
      List(
        ("Andorre", "2.0"),
        ("Belgique", "2.5")
      )
    ).toDF("adversaire", "avg_france_score")

    assertDataFrameEquals(avg, expected)
  }

  test("Calculate avg of opponent's goals") {
    Given("DataFrame with french goals pert match")
    val base = spark.sparkContext.parallelize(
      List(
        ("Andorre", "3"),
        ("Andorre", "1"),
        ("Andorre", "2"),
        ("Belgique", "5"),
        ("Belgique", "0")
      )
    ).toDF("adversaire", "score_adversaire")

    When("I call avgGoalOpponent")
    val avg = FootballApp.avgGoalOpponent(base)

    Then("I get the correct average")
    val expected = spark.sparkContext.parallelize(
      List(
        ("Andorre", "2.0"),
        ("Belgique", "2.5")
      )
    ).toDF("adversaire", "avg_adversaire_score")

    assertDataFrameEquals(avg, expected)
  }

  test("Count number of matches played by one opponent") {
    Given("Dataframe with all matches")
    val base = spark.sparkContext.parallelize(
      List(
        ("Andorre", "France - Andorre"),
        ("Andorre", "Andorre - France"),
        ("Andorre", "France - Andorre"),
        ("Belgique", "Belgique - France"),
        ("Belgique", "France - Belgique")
      )
    ).toDF("adversaire", "match")

    When("I call the function countMatchesPlayed")
    val count = FootballApp.countMatchPlayed(base)

    Then("I get the correct count")
    val expected = spark.sparkContext.parallelize(
      List(
        ("Andorre", 3),
        ("Belgique", 2)
      )
    ).toDF("adversaire", "match_count")

    assertDataFrameEquals(count, expected)
  }

  test("percent") {

  }

  test("Count of matches played during world cup") {
    Given("Dataset with matches")
    val base = spark.sparkContext.parallelize(
      List(
        ("Coupe du monde 1994", "Andorre"),
        ("Match amical", "Andorre"),
        ("Match amical", "Belgique"),
        ("Coupe du monde 1998", "Andorre"),
        ("Coupe du monde 1998", "Belgique")
      )
    ).toDF("competition", "adversaire")

    When("I call the function countMatchesWorldCup")
    val count = FootballApp.countMatchesWorldCup(base)

    Then("I get the number of matches during world cup")
    val expected = spark.sparkContext.parallelize(
      List(
        ("Andorre", 2),
        ("Belgique", 1)
      )
    ).toDF("adversaire", "count_world_cup")

    assertDataFrameEquals(count, expected)
  }

  test("Max penalty France's got") {
    Given("Dataset with penalties")
    val base = spark.sparkContext.parallelize(
      List(
        ("Andorre", "2"),
        ("Andorre", "0"),
        ("Belgique", "0"),
        ("Andorre", "4"),
        ("Belgique", "1")
      )
    ).toDF("adversaire", "penalty_adversaire")

    When("I call the function maxFrancePenalty")
    val max = FootballApp.maxFrancePenality(base)

    Then("I get the max penalty France got vs another team")
    val expected = spark.sparkContext.parallelize(
      List(
        ("Andorre", 4),
        ("Belgique", 1)
      )
    ).toDF("adversaire", "max_penalty_france")

    assertDataFrameEquals(max, expected)
  }

  test("Substract penalty from France and opponent") {
    Given("Dataset with penalties")
    val base = spark.sparkContext.parallelize(
      List(
        ("Andorre", "2", "3"),
        ("Andorre", "1", "0"),
        ("Andorre", "4", "3"),
        ("Belgique", "0", "1"),
        ("Belgique", "1", "4")
      )
    ).toDF("adversaire", "penalty_france", "penalty_adversaire")

    When("I call the function maxFrancePenalty")
    val max = FootballApp.calculPenaltyDifference(base)

    Then("I get the max penalty France got vs another team")
    val expected = spark.sparkContext.parallelize(
      List(
        ("Andorre", -1),
        ("Belgique", 4)
      )
    ).toDF("adversaire", "penalty_diff")

    assertDataFrameEquals(max, expected)
  }

  test("Verify partitionning parquet is working"){
    Given(" 2 dataframes, clean and stats")
    val clean = spark.sparkContext.parallelize(
      List(
        ("Andorre", "1998-01-02"),
        ("Belgique", "1998-01-03"),
        ("Japon", "2020-04-03")
      )
    ).toDF("adversaire", "date")


    val stats = spark.sparkContext.parallelize(
      List(
        ("Andorre", 3),
        ("Belgique", 2),
        ("Japon", 1)
      )
    ).toDF("adversaire", "match_count")

    val write = "src/test/resources/parquet/join.parquet/"
    val read = "src/test/resources/parquet/join.parquet/year=1998"

    When("Calling joinCleanStats function")
    FootballApp.joinCleanStats(clean, stats, write)

    Then("Reading will return 2 lines")
    val actually = spark.sqlContext.read.parquet(read)

    assert(actually.count() === 2 )
  }
}

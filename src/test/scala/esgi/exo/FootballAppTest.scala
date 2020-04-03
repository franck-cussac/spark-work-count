package esgi.exo

import org.apache.spark.sql
import org.scalatest.{FunSuite, GivenWhenThen}
import spark.{DataFrameAssertions, SharedSparkSession}

import scala.reflect.io.Directory
import java.io.File
import java.util.Calendar

class FootballAppTest extends FunSuite with GivenWhenThen with DataFrameAssertions {
  val spark = SharedSparkSession.sparkSession
  import spark.implicits._

  test("je veux nettoyer mes données en renommant, selectionnant, complétant et filtrant") {
      Given("dataframe avec X2,X4,X5,X6,adversaire,score_france,score_adversaire,penalty_france,penalty_adversaire,date,year,outcome,no")

      val input = spark.read.option("sep", ",").option("header", true).csv("src/test/resources/df_matches.csv")
      val expected = spark.sparkContext.parallelize(
        List(
          ("Danemark - France" , "Coupe du monde 2018 (Groupe C)", "Danemark", 0,0, 0, 0, "2018-06-26")
        )
      ).toDF("match", "competition","adversaire","score_france","score_adversaire","penalty_france","penalty_adversaire","date")

      When("rename X4 et X6, ne garder que les dates valides, mettre des 0 dans les penalty non assigné, ne garder que les matchs apres mars 1980")
      val actual = FootballApp.firstClean(input)

      Then("return ok")
      assertDataFrameEquals(actual, expected)
    }

  test("je veux vérifier que la France soit à domicile quand le match commence par France"){
    val input = "France - Belgique"

    val actually = FootballApp.udfDomicile(input)

    val expected = true

    assert(actually == expected)
  }

  test("je veux vérifier que la France soit à l'extérieur quand le match ne commence pas par France"){
    val input = "Belgique - France"

    val actually = FootballApp.udfDomicile(input)

    val expected = false

    assert(actually == expected)
  }

  test("je veux créer des statistiques des aversaires") {
    Given("dataframe avec \"match\", \"competition\",\"adversaire\",\"score_france\",\"score_adversaire\",\"penalty_france\",\"penalty_adversaire\",\"date\", \"domicile\"")
    val input = spark.sparkContext.parallelize(
      List(
        ("Russie - France" , "Match amical", "Russie", 3,1, 0, 0, "2018-06-26", false),
          ("France - Irlande" , "Match amical", "Irlande", 2,2, 4, 5, "2018-06-26", true),
          ("Italie - France" , "Coupe du monde 2006 (Finale)", "Italie", 1,1, 3, 5, "2006-07-09",false)
      )
    ).toDF("match", "competition","adversaire","score_france","score_adversaire","penalty_france","penalty_adversaire","date", "domicile")
    val expected = spark.sparkContext.parallelize(
      List(
        ("Russie" , 3,1, 1,0, 0, 0, 0),
        ("Irlande" , 2,2, 1,100, 0, 5, 1),
        ("Italie" , 1,1, 1,0, 1, 5, 2)
      )
    ).toDF("adversaire", "avg_france_pt","avg_adv_pt","nb_match","percent_domicile","nb_coupe","most_penalty","diff_penalty")
    When("calcule de stats des adversaires")
    val actual = FootballApp.adversairesStats(input)

    Then("return ok")
    assertDataFrameEquals(actual, expected)
  }

  test("je veux vérifier que la sauvegarde en parquet des statistiques soit bonne"){
    Given("given un fichier source de match")
    val input = "src/test/resources/df_matches_int.csv"

    val expected = spark.sparkContext.parallelize(
      List(
        ("Russie" , 3,1, 1,0, 0, 0, 0),
        ("Irlande" , 2,2, 1,100, 0, 5, 1),
        ("Italie" , 1,1, 1,0, 1, 5, 2)
      )
    ).toDF("adversaire", "avg_france_pt","avg_adv_pt","nb_match","percent_domicile","nb_coupe","most_penalty","diff_penalty")

    val df = spark.read.option("sep", ",").option("header", true).csv(input)
    val output = "src/test/resources/output/stats_tests.parquet"

    When("clean & calcul stats")
    FootballApp.launcher(df, output)

    val actually = spark.sqlContext.read.parquet(output)

    Then("Les dataframes correspondent")
    assertDataFrameEquals(actually, expected)
  }

  test("je veux vérifier que la jointure et que le parquet partitionné soient valides"){
    Given(" 2 dataframes left right à joindre")
    val inputLeft = spark.sparkContext.parallelize(
      List(
        ("Russie - France" , "Match amical", "Russie", 3,1, 0, 0, "2018-06-26", false),
        ("France - Irlande" , "Match amical", "Irlande", 2,2, 4, 5, "2018-06-26", true),
        ("Italie - France" , "Coupe du monde 2006 (Finale)", "Italie", 1,1, 3, 5, "2006-07-09",false)
      )
    ).toDF("match", "competition","adversaire","score_france","score_adversaire","penalty_france","penalty_adversaire","date", "domicile")

    val inputRight = spark.sparkContext.parallelize(
      List(
        ("Russie" , 3,1, 1,0, 0, 0, 0),
        ("Irlande" , 2,2, 1,100, 0, 5, 1),
        ("Italie" , 1,1, 1,0, 1, 5, 2)
      )
    ).toDF("adversaire", "avg_france_pt","avg_adv_pt","nb_match","percent_domicile","nb_coupe","most_penalty","diff_penalty")

    val output = "src/test/resources/output/join_test/"
    val sub_output = "src/test/resources/output/join_test/year=2018"

    When("Parquet selon date et mois")
    FootballApp.joinSource(inputLeft, inputRight, output)
    val actually = spark.sqlContext.read.parquet(sub_output)

    Then("Le nombre de ligne du sous parquet correspond")
    assert(actually.count() === 2 )
  }

}

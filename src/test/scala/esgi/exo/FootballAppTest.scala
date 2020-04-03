package esgi.exo

import org.scalatest.{FunSuite, GivenWhenThen}
import spark.{DataFrameAssertions, SharedSparkSession}

class FootballAppTest extends FunSuite with GivenWhenThen with DataFrameAssertions {
  val spark = SharedSparkSession.sparkSession
  import spark.implicits._


  test("je veux ajouter une colonne avec la moyenne des numéros département par région") {
    Given("une dataframe avec au moins 3 colonnes : nom région, code région et numéro département")


    When("On créait une colonne avec la moyenne des numéro département par code région et les colonnes code_region, avg_dep, nom_region")


    Then("On devrait obtenir expected")
  }

}

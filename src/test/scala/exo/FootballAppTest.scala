package exo

import esgi.exo.FootballApp
import org.apache.spark.sql.SparkSession
import org.scalatest.{FunSuite, GivenWhenThen}
import spark.{DataFrameAssertions, SharedSparkSession}

class FootballAppTest extends FunSuite with GivenWhenThen with DataFrameAssertions {
  val sparkSession: SparkSession = SharedSparkSession.sparkSession

  test("retour de 0 ou 1 selon les valeurs d'un penalty") {
    Given("la liste des valeurs possibles")
    val input = List("1", "NA", null)

    When("j'appelle la gestion des penalties")
    val actual = input.map(FootballApp.manageNullPenalties)

    val expected = List(1, 0, 0)
    actual should contain theSameElementsAs expected
  }


  test("je vérifie si une liste retourne bien si c'est à domicile ou pas") {
    Given("Une liste de chaîne de caractères")
    val input = List("France - Italie", "Italie - France", "Italie - Espagne", null)

    When("j'appelle la gestion des matchs à domicile")
    val actual = input.map(FootballApp.home)

    Then("Une liste de booléen")
    val expected = List(true, false, false, false)

    actual should contain theSameElementsAs expected
  }
}

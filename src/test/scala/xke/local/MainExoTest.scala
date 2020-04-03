package xke.local

import org.scalatest.{FunSuite, GivenWhenThen}
import spark.{DataFrameAssertions, SharedSparkSession}

class MainExoTest extends FunSuite with GivenWhenThen with DataFrameAssertions {
    val spark = SharedSparkSession.sparkSession

    import spark.implicits._

    /*test("main must create a file with word count result") {
      Given("input filepath and output filepath")
      val input = "src/test/resources/input.txt"
      val output = "src/test/resources/output/v1/parquet"

      When("I call word count")
      HelloWorld.main(Array(input, output))
      val expected = spark.sparkContext.parallelize(
        List(("rapidement",1),
          ("te",1),
          ("à",1),
          ("mots",1),
          ("des",1),
          ("s'il",1),
          ("compter",1),
          ("Bonjour,",1),
          ("as",1),
          ("plait.",1),
          ("tu",1))
      ).toDF("word", "count")

      Then("I can read output file and find my values")
      val actually = spark.sqlContext.read.parquet(output)

      assertDataFrameEquals(actually, expected)
    }*/

    test("je veux ajouter une colonne avec la moyenne des numéros département par région") {
        Given("input et expected")
        val input = spark.sparkContext.parallelize(List(
            (1, 75, "idf"),
            (1, 77, "idf"),
            (1, 78, "idf"),
            (1, 91, "idf"),
            (1, 92, "idf"),
            (1, 93, "idf"),
            (1, 94, "idf"),
            (1, 95, "idf")
        )).toDF("code_region", "code_departement", "nom_region")

        val expected = spark.sparkContext.parallelize(List(
            (1, 86.875, "idf")
        )).toDF("code_region", "avg(code_departement)", "nom_region")

        When("Calcul de la moyenne des numéros de départements par région")
        val actual = MainExo.avgDepByReg(input)

        Then("Retrouver les moyennes par régions des départements")
        assertDataFrameEquals(actual, expected)
    }

    test("je veux renommer la colonne des moyennes des numéros département") {
        Given("input et expected")
        val input = spark.sparkContext.parallelize(List(
            (1, 75, "idf"),
            (1, 77, "idf"),
            (1, 78, "idf"),
            (1, 91, "idf"),
            (1, 92, "idf"),
            (1, 93, "idf"),
            (1, 94, "idf"),
            (1, 95, "idf")
        )).toDF("code_region", "code_departement", "nom_region")

        val expected = spark.sparkContext.parallelize(List(
            (1, 86.875, "idf")
        )).toDF("code_region", "avg_dep", "nom_region")

        When("Calcul de la moyenne des numéros de départements par région et renomme colonne")
        var actual = MainExo.avgDepByReg(input)
        actual = MainExo.renameColumn(actual)

        Then("Colonne renommée")
        assertDataFrameEquals(actual, expected)
    }

    test("je veux vérifier que je lis un fichier, ajoute une colonne, la renomme, et sauvegarde mon fichier en parquet") {
        Given("Write parquet")
        var df = spark.read.option("delimiter", ",").option("header", true).csv("src/main/resources/departements-france.csv")
        df = MainExo.avgDepByReg(df)
        df = MainExo.renameColumn(df)
        df.write.parquet("avgdep-parquet")

        When("Read parquet")
        val parquet = spark.read.parquet("avgdep-parquet")

        Then("Meme DataFrame")
        assertDataFrameEquals(parquet, df)
    }

    test("je teste removeZeroAndToInt avec 06") {
        val input = "06"
        val expected = 6
        val actual = MainExo._removeZeroAndToInt(input)

        actual.shouldEqual(expected)
    }

    test("je teste removeZeroAndToInt avec 2A") {
        val input = "2A"
        val expected = 2
        val actual = MainExo._removeZeroAndToInt(input)

        actual.shouldEqual(expected)
    }

    test("je teste removeZeroAndToInt avec 2B") {
        val input = "2B"
        val expected = 2
        val actual = MainExo._removeZeroAndToInt(input)

        actual.shouldEqual(expected)
    }
}

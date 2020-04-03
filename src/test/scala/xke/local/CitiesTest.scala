package xke.local

import java.io.File

import org.scalatest.{FunSuite, GivenWhenThen}
import spark.{DataFrameAssertions, SharedSparkSession}

import scala.reflect.io.Directory

class CitiesTest extends FunSuite with GivenWhenThen with DataFrameAssertions {
    val spark = SharedSparkSession.sparkSession

    test("citiesFolder.exists shouldBe(true)") {
        Cities.main(null)

        var citiesFolder = new Directory(new File("./cities-parquet"))

        citiesFolder.exists shouldBe true
    }

    test("citiesFolder.dirs.length should be > 0") {
        Cities.main(null)

        var citiesFolder = new Directory(new File("./cities-parquet"))

        citiesFolder.dirs.length should be > 0
    }

    test("citiesFolder.dirs.length should be 19") {
        Cities.main(null)

        var citiesFolder = new Directory(new File("./cities-parquet"))

        citiesFolder.dirs.length shouldEqual 19
    }

    test("citiesFolder.dirs.length should be 8") {
        Cities.main(null)

        var citiesFolder = new Directory(new File("./cities-parquet/p_region_code=11"))

        citiesFolder.dirs.length shouldEqual 8
    }
}
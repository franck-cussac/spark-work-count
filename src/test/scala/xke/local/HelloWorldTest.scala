package xke.local

import org.apache.spark.sql.functions.col
import org.scalatest.{FunSuite, GivenWhenThen}
import spark.{DataFrameAssertions, SharedSparkSession}
import xke.local.HelloWorld.{cleanDf, renameColumn}

class HelloWorldTest extends FunSuite with GivenWhenThen with DataFrameAssertions {
  val spark = SharedSparkSession.sparkSession
  import spark.implicits._

  test("je veux ajouter une colonne avec la moyenne des numéros département par région") {
    Given("dataframe avec 3 colonnes : nom région, code région, numé département")
    val input = spark.sparkContext.parallelize(
      List(
        ("Ile de france", 10 , 75),
        ("Ile de france", 10 , 75),
        ("Ile de france", 10 , 75),
        ("Aquitaine", 20 , 50)
      )
    ).toDF("nom_region", "code_region", "code_departement")


    val expected = spark.sparkContext.parallelize(
      List(
        (10 , 75, "Ile de france"),
        ( 20 , 50, "Aquitaine")
      )
    ).toDF("code_region", "avg_departement", "nom_region")

    When("calcule average")
    val actual = HelloWorld.avgDepByReg(input)

    Then("return ok")
    assertDataFrameEquals(actual, expected)
  }

  test("je veux renommer la colonne des moyennes des numéros département") {

    Given("dataframe avec 3 colonnes : nom région, code région, numé département")
    val input = spark.sparkContext.parallelize(
      List(
        (32 , 2, "Hauts-de-France"),
        ( 20 , 50, "Aquitaine")
      )
    ).toDF("code_region", "avg(code_departement)", "nom_region")


    val expected = spark.sparkContext.parallelize(
      List(
        (32 , 2, "Hauts-de-France"),
        ( 20 , 50, "Aquitaine")
      )
    ).toDF("code_region", "avg_dep", "nom_region")

    When("rename column")
    val actual = HelloWorld.renameColumn(input, "avg_dep", "avg(code_departement)" )

    Then("column renamed")
    assertDataFrameEquals(actual, expected)
  }

  test("je veux vérifier que je lis un fichier, ajoute une colonne, la renomme, et sauvegarde mon fichier en parquet") {
    val df = spark.read.option("sep", ",").option("header", true).csv("src/main/resources/departements-france.csv")
    HelloWorld.firstWork(df)

    val actually = spark.read.parquet("src/main/data/region/")

    val dfClean = cleanDf(df)
    val av = HelloWorld.avgDepByReg(dfClean)
    val expected = HelloWorld.renameColumn(av, "avg_dep", "avg_departement")

    assertDataFrameEquals(actually, expected)
  }

  test("je veux vérifier que je joins les trois fichiers et sauvegardes mon fichier en parquet") {
    val dfCities = spark.sparkContext.parallelize(
      List(
        (1,"1","01001","01400","L'Abergement-Clémenciat","l abergement clemenciat",46.156781992032,4.9246992031872),
        (2,"1","01002","01640","L'Abergement-de-Varey","l abergement de varey",46.010085625,5.4287591666667),
        (938,"2","02543","02470","Neuilly-Saint-Front","neuilly saint front",49.167268791946,3.2576386577181)
      )
    ).toDF("id","department_code","insee_code","zip_code","name","slug","gps_lat","gps_lng")
    val dfDepartments = spark.sparkContext.parallelize(
      List(
        (1,84,"1","Ain","ain"),
        (2,32,"2","Aisne","aisne")
      )
    ).toDF("id","region_code","code","name","slug")
    val dfRegions = spark.sparkContext.parallelize(
      List(
        (16,84,"Auvergne-Rhône-Alpes","auvergne rhone alpes"),
        (10,32,"Hauts-de-France","hauts de france")
      )
    ).toDF("id","code","name","slug")
    HelloWorld.secondWork(dfCities, dfDepartments, dfRegions)

    val actually = spark.read.parquet("src/main/data/region_2/")

    val cityDfClean = renameColumn(
      renameColumn(dfCities, "city_name", "name"),
      "city_slug",
      "slug"
    )
    val departmentDfClean = renameColumn(
      renameColumn(
        renameColumn(dfDepartments, "department_name", "name"),
        "department_id",
        "id"
      ),
      "department_slug",
      "slug"
    )
    val regionDfClean = renameColumn(
      renameColumn(
        renameColumn(dfRegions, "region_name", "name"),
        "region_id",
        "id"
      ),
      "region_slug",
      "slug"
    )

    val expected = cityDfClean.join(
      departmentDfClean
      , col("department_code") === departmentDfClean.col("code")
      , "inner"
    ).join(
      regionDfClean
      , col("region_code") === regionDfClean.col("code")
      , "inner"
    ).drop("code")

    assertDataFrameEquals(actually, expected)
  }

  test("je veux vérifier que je nettoie bien mes code departement"){
    val input = spark.sparkContext.parallelize(
      List(
        ("02" , 2, "Hauts-de-France"),
        ("32" , 2, "Hauts-de-France"),
        ("2a" , 2, "Hauts-de-France"),
        ( "30" , 50, "Aquitaine")
      )
    ).toDF("code_departement", "avg(code_departement)", "nom_region")

    val actually = HelloWorld.cleanDf(input)

    val expected = spark.sparkContext.parallelize(
      List(
        (2, 2, "Hauts-de-France"),
        (32, 2, "Hauts-de-France"),
        (2, 2, "Hauts-de-France"),
        (30, 50, "Aquitaine")
      )
    ).toDF("code_departement", "avg(code_departement)", "nom_region")

    assertDataFrameEquals(actually, expected)
  }
}

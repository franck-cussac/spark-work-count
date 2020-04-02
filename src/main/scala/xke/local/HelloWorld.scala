package xke.local

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object HelloWorld {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate()

    // code
    // src/main/resources/departements-france.csv
    // 1) lire le fichier
    // 2) créer une colonne avec la moyenne des numéro département par code région
    //    code_region, avg_dep, nom_region
    // 3) renommer la colonne moyenne des départements en avg_dep
    // 4) écrire le fichier en parquet

    val df = spark.read.option("sep", ",").option("header", true).csv("src/main/resources/departements-france.csv")
    firstWork(df).show

    val dfCities = spark.read.option("sep", ",").option("header", true).csv("src/main/resources/cities.csv")
    val dfDepartements = spark.read.option("sep", ",").option("header", true).csv("src/main/resources/departments.csv")
    val dfRegions = spark.read.option("sep", ",").option("header", true).csv("src/main/resources/regions.csv")
    secondWork(dfCities, dfDepartements, dfRegions).show

  }

  def avgDepByReg(dataFrame: DataFrame): DataFrame = {
    dataFrame.withColumn("code_departement", dataFrame.col("code_departement")
      .cast("Double"))
      .groupBy(col("code_region"))
      .agg(avg("code_departement").as("avg_departement"), first("nom_region").as("nom_region"))
  }

  def renameColumn(dataFrame: DataFrame, newName: String, oldName: String): DataFrame = {
    dataFrame.withColumnRenamed(oldName, newName)
  }

  def cleanDf(dataFrame: DataFrame): DataFrame = {
    val stringAdapter = (s: String) => {
      val Pattern = "0".r

      s.replaceAll("[^\\d.]", "") match {
        case Pattern(c) => c.charAt(1).toInt
        case c => c.toInt
      }
    }

    val stringUdf = udf(stringAdapter)
    dataFrame.withColumn("code_departement", stringUdf(col("code_departement")) as "code_departement")
  }

  def firstWork(dataFrame: DataFrame): DataFrame = {
    val dfClean = cleanDf(dataFrame)
    dfClean.show()
    val avg = avgDepByReg(dfClean)
    avg.show

    val renamed = renameColumn(avg, "avg_dep", "avg_departement")
    renamed.write.mode("overwrite").parquet("src/main/data/region/")
    renamed
  }

  def secondWork(dataFrameCities: DataFrame, dataFrameDepartment: DataFrame, dataFrameRegions: DataFrame): DataFrame = {
    val cityDfClean = renameColumn(
      renameColumn(dataFrameCities, "city_name", "name"),
      "city_slug",
      "slug"
    )
    val departmentDfClean = renameColumn(
      renameColumn(
        renameColumn(dataFrameDepartment, "department_name", "name"),
        "department_id",
        "id"
      ),
      "department_slug",
      "slug"
    )
    val regionDfClean = renameColumn(
      renameColumn(
        renameColumn(dataFrameRegions, "region_name", "name"),
        "region_id",
        "id"
      ),
      "region_slug",
      "slug"
    )

    val joined_df = cityDfClean.join(
      departmentDfClean
      , col("department_code") === departmentDfClean.col("code")
      , "inner"
    ).join(
      regionDfClean
      , col("region_code") === regionDfClean.col("code")
      , "inner"
    ).drop("code")

    joined_df.write.mode("overwrite").parquet("src/main/data/region_2/")
    joined_df
  }
}

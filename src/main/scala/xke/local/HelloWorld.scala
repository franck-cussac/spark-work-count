package xke.local

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object HelloWorld {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("test").getOrCreate()
    import spark.implicits._

    // code
    // src/main/resources/departements-france.csv
    // 1) lire le fichier
    val df1 = spark.read
      .option("delimiter", ",")
      .option("header", true)
      .option("inferSchema", true)
      .csv("src/main/resources/departements-france.csv")
      // 2) garder les codes régions pairs
      .filter(col("code_region") % 2 === 0)
      // 3) regrouper les régions par dizaine de code région
      .groupBy(col("code_region")).count()
      // 4) ne garder que les lignes avec plus de 5 résultats de régions
      .filter(col("count") > 5)

    // 5) afficher le nombre de lignes finales
    df1.show()

    // code
    // src/main/resources/departements-france.csv
    // 1) lire le fichier
    val df2 = spark.read
      .option("delimiter", ",")
      .option("header", true)
      .option("inferSchema", true)
      .csv("src/main/resources/departements-france.csv")
    // 2) créer une colonne avec la moyenne des numéros de département par code région
    val df2_avgDepByReg = HelloWorld.avgDepByReg(df2)
      // code_region, avg_dep, nom_region
    // 3) renommer la colonne moyenne des numéros de département en avg_dep
    val df2_renameColumn = HelloWorld.renameColumns(
      df2_avgDepByReg,
      Map("avg(code_departement)" -> "avg_dep")
    )
    // 4) afficher le nombre de lignes finales
    df2_renameColumn.show()

    // code
    // créer une UDF
    // 1) qui prend un String en paramètre et renvoie un Int
    // 2) si votre String commence par un 0, on l'enlève
    // 3) si votre String contient un caractère non numérique, on l'enlève
    // 4) faire un test unitaire également

    // code
    // jointure au niveau des numéros de département entre le fichier regions.csv + departments.csv et cities.csv + departement.csv
    val df3_departements = spark.read
      .option("delimiter", ",")
      .option("header", true)
      .option("inferSchema", true)
      .csv("src/main/resources/departments.csv");

    val df3_departments_renamed = HelloWorld.renameColumns(
      df3_departements,
      Map(
        "id" -> "department_id",
        "region_code" -> "department_region_code",
        "code" -> "department_code",
        "name" -> "department_name",
        "slug" -> "department_slug"
      )
    )

    val df3_regions = spark.read
      .option("delimiter", ",")
      .option("header", true)
      .option("inferSchema", true)
      .csv("src/main/resources/regions.csv")

    val df3_regions_renamed = HelloWorld.renameColumns(
      df3_regions,
      Map(
        "id" -> "region_id",
        "code" -> "region_code",
        "name" -> "region_name",
        "slug" -> "region_slug"
      )
    )

    val df3_cities = spark.read
      .option("delimiter", ",")
      .option("header", true)
      .option("inferSchema", true)
      .csv("src/main/resources/cities.csv")

    val df3_cities_renamed = HelloWorld.renameColumns(
      df3_cities,
      Map(
        "id" -> "city_id",
        "department_code" -> "city_department_code",
        "insee_code" -> "city_insee_code",
        "zip_code" -> "city_zip_code",
        "name" -> "city_name",
        "slug" -> "city_slug",
        "gps_lat" -> "city_gps_lat",
        "gps_lng" -> "city_gps_lng"
      )
    )

    val df3_departments_union_regions = df3_departments_renamed.join(
      df3_regions_renamed,
      df3_departments_renamed("department_region_code") === df3_regions_renamed("region_code"),
      "left"
    )

    val df3_departements_union_regions_union_cities = df3_cities_renamed.join(
      df3_departments_union_regions,
      df3_cities_renamed("city_department_code") === df3_departments_union_regions("department_code"),
      "left"
    )

    df3_departements_union_regions_union_cities.show()
    df3_departements_union_regions_union_cities.write.mode(SaveMode.Overwrite).parquet("src/resources/output/v1/departments_regions_cities.parquet")
  }

  def extractCode(code: String): Int = {
    code match {
      case c if c.head == '0' => c.substring(1).toInt
      case c if "AB" contains code.last => c.dropRight(1).toInt
      case c => c.toInt
    }
  }

  def avgDepByReg(df: DataFrame): DataFrame = {
    val extractCodeUdf = udf(extractCode _)

    df
      .withColumn("code_departement", extractCodeUdf(col("code_departement")))
      .groupBy("code_region", "nom_region")
      .avg("code_departement")
  }

  /* def renameColumn(df: DataFrame, selectedColumn: String, newColumn: String): DataFrame = {
    HelloWorld.renameColumns(df, Map(selectedColumn -> newColumn))
  } */

  def renameColumns(df: DataFrame, selectedColumns: Map[String, String]): DataFrame = {
    selectedColumns.foldLeft(df)((acc, item) => acc.withColumnRenamed(item._1, item._2))
  }
}

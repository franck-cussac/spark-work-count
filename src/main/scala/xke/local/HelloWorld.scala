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
    val df = spark.read
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
    df.show()

    // code
    // src/main/resources/departements-france.csv
    // 1) lire le fichier
    // 2) créer une colonne avec la moyenne des numéros de département par code région
      // code_region, avg_dep, nom_region
    // 3) renommer la colonne moyenne des numéros de département en avg_dep
    // 4) écrire le fichier en parquet
    val testDf = spark.read
      .option("delimiter", ",")
      .option("header", true)
      .option("inferSchema", true)
      .csv("src/main/resources/departements-france.csv")

    testDf.show()

    HelloWorld.avgDepByReg(testDf).show()
  }

  def avgDepByReg(df: DataFrame): DataFrame = {
    df
      .withColumn("code_departement", col("code_departement").cast("integer"))
      // .filter(row => row.getAs[String]("code_departement").matches("""\d+"""))
      .groupBy("code_region")
      .agg(
        avg("code_departement").as("avg_dep")
      )
  }

  def renameColumn(df: String): DataFrame = ???
}

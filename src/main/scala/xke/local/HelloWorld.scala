package xke.local

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object HelloWorld {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate()
    import spark.implicits._

    // code
    // src/main/resources/departements-france.csv
    // 1) lire le fichier
    // 2) créer une colonne avec la moyenne des numéro département par code région
    //    code_region, avg_dep, nom_region
    // 3) renommer la colonne moyenne des départements en avg_dep
    // 4) écrire le fichier en parquet

    // main classique
    if (args.length == 0) {
      val dfDepts = getDFDepartments(spark)
      val dfCities = getDFCities(spark)
      writeParquet(
        renameColumn(
          avgDepByReg(dfDepts)
        )
      )

      val jointure = joinDeptsAndCities(dfDepts, dfCities)
      writeParquetByRegionAndDept(jointure)
    }

    // pour les tests
    else {
      mainTest(spark)
    }
  }

  def joinDeptsAndCities(dfDepts: DataFrame, dfCities: DataFrame): DataFrame = {
    // jointure sur le code du département + on supp une des deux cononnes, inutile
    dfDepts.join(dfCities,
      dfDepts("code_departement") === dfCities("department_code"),
      "left_outer"
    )
      .drop("department_code")
  }

  def getDFDepartments(spark: SparkSession): DataFrame = {
    spark.read.option("header", true).csv("src/main/resources/departements-france.csv")
      .withColumn("code_departement", extractDepInt(col("code_departement")))
  }

  def getDFCities(spark: SparkSession): DataFrame = {
    spark.read.option("header", true).csv("src/main/resources/cities.csv")
      .withColumn("department_code", extractDepInt(col("department_code")))
  }

  def mainTest(spark: SparkSession): Unit = {
    import spark.implicits._
    val df = List(
      (1, 2, "toto"),
      (1, 3, "toto"),
      (1, 4, "toto"),
      (2, 14, "zaza"),
      (2, 54, "zaza"),
      (2, 7, "zaza"),
      (2, 5, "zaza")
    ).toDF("code_region", "code_departement", "nom_region")
      .withColumn("code_departement_int", extractDepInt(col("code_departement")))
    writeParquet(
      renameColumn(
        avgDepByReg(df)
      )
    )
  }

  def avgDepByReg(df: DataFrame): DataFrame = {
    df.groupBy("code_region", "nom_region")
      .agg(avg("code_departement"))
  }
  def renameColumn(df: DataFrame): DataFrame = {
    df.withColumnRenamed("avg(code_departement)", "avg_dep")
  }
  def writeParquet(df: DataFrame): Unit = {
    df.write.mode("overwrite").parquet("src/main/parquets/output.parquet")
  }
  def writeParquetByRegionAndDept(df: DataFrame): Unit = {
    df.write.partitionBy("code_region", "code_departement").mode("overwrite").parquet("src/main/parquets/byRegionAndDepts.parquet")
  }

  // créer une UDF :
  //1) qui prend un String en paramètre et renvoie un Int
  //2) si votre String commence par un 0, on l'enlève
  //3) si votre String contient un caractère non numérique, on l'enlève
  //4) rédiger un test unitaire sur la fonction
  def stringToInt: String => Int = s => s.filter(Character.isDigit).toInt
  val extractDepInt: UserDefinedFunction = udf(stringToInt)
}

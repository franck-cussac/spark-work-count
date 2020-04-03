package xke.local

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object HelloWorld {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").appName("Salut Monde").getOrCreate()

    // 1) lire le fichier src/main/resources/departements-france.csv
    // 2) garder les codes régions pairs
    // 3) regrouper les régions par dizaine de code région
    // 4) ne garder que les lignes avec plus de 5 résultats de régions
    val df = sparkSession.read.option("delimiter", ",").option("header", value = true).csv("src/main/resources/departements-france.csv")
    val dfPar = df.filter(col("code_region") % 2 === 0)
    val dfGroup = dfPar.groupBy(col("code_region")).count()
    val dfHaving = dfGroup.filter(col("count") > 5)
    dfHaving.show()


    // 1) lire le fichier rc/main/resources/departements-france.csv
    // 2) créer une colonne avec la moyenne des numéros de département par code région
    // 3) renommer la colonne moyenne des numéros de département en avg_dep
    val df2 = sparkSession.read.option("delimiter", ",").option("header", value = true).csv("src/main/resources/departements-france.csv")
    val dfAverage = getAverageDepartmentNumberByRegion(df2)
    val dfRenamed = renameAverageColumn(dfAverage)
    writeToParquet(dfRenamed)
    dfRenamed.show()

    // User Defined Function
    // 1) qui prend un String en paramètre et renvoie un Int
    // 2) si votre String commence par un 0, on l'enlève
    // 3) si votre String contient un caractère non numérique, on l'enlève
    def toInteger(input: String): Int = {
      input.filter(Character.isDigit).toInt
    }

    val toIntegerUdf: UserDefinedFunction = udf(toInteger _)

    // jointure au niveau des numéros de département entre le fichier regions.csv + departments.csv et cities.csv + departement.csv
    val dfDepartments = sparkSession.read.option("header", value = true).csv("src/main/resources/departements-france.csv")
      .withColumn("code_departement", toIntegerUdf(col("code_departement")))
    val dfCities = sparkSession.read.option("header", value = true).csv("src/main/resources/cities.csv")
      .withColumn("department_code", toIntegerUdf(col("department_code")))

    val dfJoin = dfDepartments
      .join(dfCities, col("code_departement") === col("department_code"), "left_outer")
      .drop("department_code")
    dfJoin.show()
  }

  def getAverageDepartmentNumberByRegion(input: DataFrame): DataFrame = {
    input.groupBy("code_region", "nom_region")
      .agg(avg("code_departement"))
  }

  def renameAverageColumn(input: DataFrame): DataFrame = {
    input.withColumnRenamed("avg(code_departement)", "avg_dep")
  }

  def writeToParquet(input: DataFrame): Unit = {
    input.write.mode("overwrite").parquet("src/main/parquet/exo.parquet")
  }
}

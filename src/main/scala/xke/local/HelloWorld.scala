package xke.local

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object HelloWorld {

  val DEFAULT_CITIES_DATA_PATH = "src/main/resources/cities.csv";
  val DEFAULT_DEPARTMENT_DATA_PATH = "src/main/resources/departements-france.csv";

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate()

    // code
    // src/main/resources/departements-france.csv
    // 1) lire le fichier
    // 2) créer une colonne avec la moyenne des numéro département par code région
    //    code_region, avg_dep, nom_region
    // 3) renommer la colonne moyenne des départements en avg_dep
    // 4) écrire le fichier en parquet
    if (args.length >= 2) {
      val result = readDataAndApplyTransformations(spark, args(0), args(1))
      result.write.mode(SaveMode.Overwrite).parquet(args(1))
      result.show()
    } else {
      val result = readDataAndApplyTransformations(spark, DEFAULT_DEPARTMENT_DATA_PATH, DEFAULT_CITIES_DATA_PATH)
      result.write.mode(SaveMode.Overwrite).parquet("result")
      result.show()
    }

  }

  private def readDataAndApplyTransformations(spark: SparkSession, dataPathDepartment: String,  dataPathCities: String): DataFrame = {
    val dfDepartment = spark.read.option("delimiter", ",").option("header", value = true).option("inferSchema", "true").csv(dataPathDepartment)

    val dfCities = spark.read.option("delimiter", ",").option("header", value = true).option("inferSchema", "true").csv(dataPathCities)

    val dfDepartmentWithIntDepartment = dfDepartment.withColumn("code_departement", toIntegerUdf(dfDepartment("code_departement")))
    val dfCitiesWithIntDepartment = dfCities.withColumn("department_code", toIntegerUdf(dfCities("department_code")))

    val dfJoinDepartmentCities = joinCitiesAndDepartment(dfDepartmentWithIntDepartment, dfCitiesWithIntDepartment)

    val dfWithCodeDepartmentAvg = avgDepByReg(dfJoinDepartmentCities)

    renameColumn(dfWithCodeDepartmentAvg, "avg(code_departement)", "avg_dep")
  }

  def joinCitiesAndDepartment(dfDepartment: DataFrame, dfCities: DataFrame): DataFrame = {
    dfDepartment.join(dfCities, col("code_departement") === col("department_code"), "left_outer")
  }

  val avgDepByReg: DataFrame => DataFrame =
    (df: DataFrame) => df.groupBy("code_region", "nom_region").agg(avg("code_departement"))

  //Utilier des static pour les noms de colonne
  def renameColumn(dataFrame: DataFrame, oldName: String, newName: String): DataFrame = {
    dataFrame.withColumnRenamed(oldName, newName)
  }

  val toIntegerUdf: UserDefinedFunction = udf(toInteger _)

  def toInteger(input: String): Int = {input.filter(Character.isDigit).toInt}


  //def toIntegerTest(string: String): Int = string.filter(Character.isDigit).toInt
}

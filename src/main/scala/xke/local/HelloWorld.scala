package xke.local

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
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

    // Get Departements and Cities
    val dfDepartements = fetchDepartements(spark)
    val dfCities = fetchCities(spark)

    // Apply avg
    val dfAvg = avgDepByReg(dfDepartements)

    val dfRename = renameColumn(dfAvg, "avg_departement", "avg_dep")

    writeDepartementInParquet(dfRename)

    // Display
    dfRename.show


//    1) utiliser List().toDF() pour créer vos dataframe d'input
//    2) assurez vous que toutes vos fonctions ont des tests
//    3) terminez bien votre main en ajoutant l'UDF développé ce matin
//    4) pensez à pull la branche master, j'ai corrigé la création du jar
//    5) pour ceux qui peuvent en local, réessayez de lancer un spark-submit avec --master spark://spark-master:7077 depuis le conteneur worker
//    Pour les autres, on verra peut être cet après-midi

    val join = mergeDepartementsAndCities(dfDepartements, dfCities)

    join.show

    writeParquetByRegionAndDept(join)
  }

  def fetchDepartements(sparkSession: SparkSession): DataFrame = {
    sparkSession.read.option("sep", ",").option("header", true).csv("src/main/resources/departements-france.csv")
  }

  def fetchCities(sparkSession: SparkSession): DataFrame = {
    sparkSession.read.option("sep", ",").option("header", true).csv("src/main/resources/cities.csv")
      .withColumn("department_code", convertStringToIntUDF(col("department_code")))
  }

  def avgDepByReg(dataFrame: DataFrame): DataFrame = {
    dataFrame.withColumn("code_departement", convertStringToIntUDF(dataFrame.col("code_departement")))
      .groupBy(col("code_region"))
      .agg(avg("code_departement").as("avg_departement"),
        first("nom_region").as("nom_region"))
  }

  val convertStringToIntUDF: UserDefinedFunction = udf(convertStringToInt _)

  def convertStringToInt(value: String): Int = {
    value.filter(Character.isDigit).toInt
  }

  def mergeDepartementsAndCities(dfDepts: DataFrame, dfCities: DataFrame): DataFrame = {
    dfDepts.join(dfCities, dfDepts("code_departement") === dfCities("department_code"), "left_outer")
      .drop("department_code")
  }

  def renameColumn(dataFrame: DataFrame, theOldName: String, theNewName: String): DataFrame = {
    dataFrame.withColumnRenamed(theOldName, theNewName)
  }

  def writeDepartementInParquet(dataFrame: DataFrame): Unit = {
    dataFrame.write
      .mode("overwrite")
      .parquet("src/main/resources/file.parquet")
  }

  def writeParquetByRegionAndDept(dataFrame: DataFrame): Unit = {
    dataFrame.write
      .partitionBy("code_region", "code_departement")
      .mode("overwrite")
      .parquet("src/main/resources/code_region-code_departement.parquet")
  }

}


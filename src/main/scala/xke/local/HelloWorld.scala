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


    // 1
    val df = fetchDepartements

    // 2
    val dfAvg = avgDepByReg(df)

    // 3
    val dfRename = renameColumn(dfAvg, "avg_departement", "avg_dep")

    // 4
    write(dfRename, "src/main/resources/file.parquet")

    // Display
    dfRename.show



//    1) utiliser List().toDF() pour créer vos dataframe d'input
//    2) assurez vous que toutes vos fonctions ont des tests
//    3) terminez bien votre main en ajoutant l'UDF développé ce matin
//      4) pensez à pull la branche master, j'ai corrigé la création du jar
//      5) pour ceux qui peuvent en local, réessayez de lancer un spark-submit avec --master spark://spark-master:7077 depuis le conteneur worker
//    Pour les autres, on verra peut être cet après-midi

  }

  val convertStringToIntUDF: UserDefinedFunction = udf(convertStringToInt _)

  def convertStringToInt(value: String): Int = {
    value.filter(Character.isDigit).toInt
  }


  def avgDepByReg(dataFrame: DataFrame): DataFrame = {
    dataFrame.withColumn("code_departement", convertStringToIntUDF(dataFrame.col("code_departement")))
      .groupBy(col("code_region"))
      .agg(avg("code_departement").as("avg_departement"),
        first("nom_region").as("nom_region"))
  }

  def renameColumn(dataFrame: DataFrame, theOldName: String, theNewName: String): DataFrame = {
    dataFrame.withColumnRenamed(theOldName, theNewName)
  }

  def write(dataFrame: DataFrame, path: String) = {
    dataFrame.write
      .mode("overwrite")
      .parquet(path)
  }

}


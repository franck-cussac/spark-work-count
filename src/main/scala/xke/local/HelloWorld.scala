package xke.local

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object HelloWorld {
  val spark: SparkSession = SparkSession.builder().master("local[*]")getOrCreate() // ctrl + Q affiche le type de la variable
  val stringToIntUdf: UserDefinedFunction = udf(stringToInt _ )

  def main(args: Array[String]): Unit = {
    executeAverageDepartmentCode()
    executeJoinBetweenDepartmentAndCity()
  }

  /**
   * Crée un dataFrame a partir de données de département
   * et créer une moyenne des code département et écrit un
   * parquet a partir du résultat
   */
  def executeAverageDepartmentCode(): Unit = {
    val df_departement = createDataFrameDepartement(spark)
    val avg_dep_df = avgDepByReg(df_departement)
    val result_df = renameColumn(avg_dep_df)
    result_df.write.mode(SaveMode.Overwrite).parquet("src/main/resources/parquet")
  }

  /**
   * Crée deux dataFrame a partir des données de département
   * et de villes, et effectue une jointure ensuite écrit un
   * parquet a partir du résultat
   */
  def executeJoinBetweenDepartmentAndCity(): Unit = {
    val df_departement = createDataFrameDepartement(spark)
    val df_cities = createDataFrameCity(spark)
    val df_joined = df_departement.join(df_cities, "code_departement")
    writeParquetFromJoinedDataFrame(df_joined)
  }

  /**
   * Ecrit sur le fileSystem le parquet a partir de dataFrame
   *
   * @param dataFrame
   */
  def writeParquetFromJoinedDataFrame(dataFrame: DataFrame): Unit = {
    dataFrame.write.mode(SaveMode.Overwrite)
      .partitionBy("code_region", "code_departement")
      .parquet("src/main/resources/parquetPartition")
  }

  /**
   * Crée une DataFrame avec le fichier departements-france.csv
   *
   * @param sparkSession
   * @return dataFrame
   */
  def createDataFrameDepartement(sparkSession: SparkSession): DataFrame = {
    sparkSession.read.option("header", true)
      .csv("src/main/resources/departements-france.csv")
      .withColumn("code_departement", stringToIntUdf(col("code_departement")))
  }

  /**
   * Crée une DataFrame avec le fichier cities.csv
   *
   * @param sparkSession
   * @return DataFrame
   */
  def createDataFrameCity(sparkSession: SparkSession): DataFrame = {
    sparkSession.read.option("header", true)
      .csv("src/main/resources/cities.csv")
      .withColumn("department_code", stringToIntUdf(col("department_code")))
      .withColumnRenamed("department_code", "code_departement")
      .withColumnRenamed("name", "nom_ville")
  }

  /**
   * Convertit une chaine de caractère en nombre entier
   *
   * @param str
   * @return le nombre entier contenu dans la chaine de caractère
   */
  def stringToInt(str: String): Int = {
    str.filter(Character.isDigit).toInt
  }

  /**
   * Regroupement des données par code_region et nom_region
   * Calcul de la moyenne des codes_departement
   *
   * @param dataFrame
   * @return
   */
  def avgDepByReg(dataFrame: DataFrame): DataFrame = {
    dataFrame.groupBy("code_region", "nom_region")
      .agg(avg("code_departement"))
  }

  /**
   * Renommage de la colonne du calcul de la moyenne
   *
   * @param dataFrame
   * @return
   */
  def renameColumn(dataFrame: DataFrame): DataFrame = {
    dataFrame.withColumnRenamed("avg(code_departement)", "avg_dep")
  }

}

package xke.local

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object HelloWorld {
  val spark = SparkSession.builder().master("local[*]")getOrCreate() // ctrl + Q affiche le type de la variable
  val stringToIntUdf: UserDefinedFunction = udf(stringToInt _ )
  import spark.implicits._

  def main(args: Array[String]): Unit = {
//    val df_departement = createDateFrameDepartement(spark)
//    val avg_dep_df = avgDepByReg(df_departement)
//    val result_df = renameColumn(avg_dep_df)

//    result_df.write.mode(SaveMode.Overwrite).parquet("/home/ubuntu/workspace/hadoop/spark-work-count/src/main/resources/parquet")

    val df_departement = createDataFrameDepartement(spark)
    val df_cities = createDataFrameCity(spark)

    val df_joined = df_departement.join(df_cities, Seq("code_departement"), "left")

    df_joined
      .write.mode(SaveMode.Overwrite)
      .partitionBy("code_region", "code_departement")
      .parquet("/home/ubuntu/workspace/hadoop/spark-work-count/src/main/resources/partition_test")
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

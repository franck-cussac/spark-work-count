package xke.local

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._


object HelloWorld {

  val app_name = "test"
  val app_master = "local[*]"
  val url_department_file = "src/main/resources/departements-france.csv"
  val url_cities_file = "src/main/resources/cities.csv"
  val url_parquet_folder = "src/main/resources/output.parquet"
  val colname_code_department = "code_departement"
  val colname_department_code = "department_code"
  val colname_code_region = "code_region"
  val colname_nom_region = "nom_region"

  val spark = SparkSession.builder()
    .appName(app_name)
    .master(app_master)
    .getOrCreate()
  import org.apache.spark.sql.functions.udf
  import spark.implicits._

  def main(args: Array[String]): Unit = {

    val dfd = getDepartmentDF()
    val dfc = getCitiesDF()
    val df_join = dfd.join(
      dfc,
      dfd(colname_code_department) === dfc(colname_department_code),
      "left_outer"
    )

    val dfd_avg = avgDepByReg(df_join)
    val dfd_renamed = renameColumn(dfd_avg, "avg(code_departement)", "avg_dep")
    dfd_renamed.show
    dfd_renamed.write.mode("overwrite").parquet(url_parquet_folder)
  }

  /**
    * retourne une dataframe sur les departements
    * @return DataFrame
    */
  def getDepartmentDF() : DataFrame = {
    spark.read.option("delimiter", ",").option("header", true)
      .csv(url_department_file)
      .withColumn(colname_code_department, toInteger(col(colname_code_department)))
  }

  /**
    * retourne une dataframe sur les villes
    * @return DataFrame
    */
  def getCitiesDF() : DataFrame = {
    spark.read.option("delimiter", ",").option("header", true)
      .csv(url_cities_file)
      .withColumn(colname_department_code, toInteger(col(colname_department_code)))
  }

  /**
    * retourne la moyenne des Departement d'une region
    * @param df
    * @return DataFrame
    */
  def avgDepByReg(df : DataFrame): DataFrame = {
    df
      .groupBy(col(colname_code_region), col(colname_nom_region))
      .agg(
        avg(colname_code_department)
      )
  }
  // a evoluer

  /**
    * renomme une colonne
    * @param df
    * @param oldName
    * @param newName
    * @return DataFrame
    */
  def renameColumn(df : DataFrame, oldName : String, newName : String): DataFrame = {
    df.withColumnRenamed(oldName, newName)
  }

  /**
    * UDF de conversion d'un String en Int
    */
  val toInteger: UserDefinedFunction = udf(convertInt _)

  /**
    * convertit un String en Int
    * @param s
    * @return Int
    */
  def convertInt(s: String) : Int = {
    if(s.startsWith("0")) {
      s.substring(1).toInt
    }
    if(s.endsWith("A") || s.endsWith("B")){
      s.substring(0,s.length-1).toInt
    } else{
      s.toInt
    }
  }
}

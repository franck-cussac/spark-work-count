package xke.local

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._


object HelloWorld {

  val app_name = "test"
  val app_master = "local[*]"
  val file_delimiter = ","
  val url_department_file = "src/main/resources/departements-france.csv"
  val url_cities_file = "src/main/resources/cities.csv"
  val url_parquet_dept_folder = "src/main/resources/parquet_dept/output.parquet"
  val url_parquet_city_folder = "src/main/resources/parquet_city/output.parquet"
  val save_mode = "overwrite"
  /***/
  val colname_code_department = "code_departement"
  val colname_department_code = "department_code"
  val colname_avg_department = "avg_dep"
  val colname_code_region = "code_region"
  val colname_nom_region = "nom_region"
  val join_type = "left_outer"
  /***/
  val spark = SparkSession.builder()
    .appName(app_name)
    .master(app_master)
    .getOrCreate()
  import org.apache.spark.sql.functions.udf
  import spark.implicits._

  def main(args: Array[String]): Unit = {

    val dfd = getDepartmentDF()
    val dfd_avg = avgDepByReg(dfd)
    val dfd_renamed = renameColumn(dfd_avg, "avg(code_departement)", colname_avg_department)
    dfd_renamed.show
    dfd_renamed.write
      .mode(save_mode)
      .parquet(url_parquet_dept_folder)

    val dfc = getCitiesDF()
    val df_join = dfd.join(
      dfc,
      dfd(colname_code_department) === dfc(colname_department_code),
      join_type
    ).drop(colname_department_code)
    df_join.show
    df_join.write
      .partitionBy(colname_code_region,colname_code_department)
      .mode(save_mode)
      .parquet(url_parquet_city_folder)
  }

  /**
    * retourne une dataframe sur les departements
    * @return DataFrame
    */
  def getDepartmentDF() : DataFrame = {
    spark.read
      .option("delimiter", file_delimiter)
      .option("header", true)
      .csv(url_department_file)
      .withColumn(colname_code_department, toInteger(col(colname_code_department)))
  }

  /**
    * retourne une dataframe sur les villes
    * @return DataFrame
    */
  def getCitiesDF() : DataFrame = {
    spark.read
      .option("delimiter", file_delimiter)
      .option("header", true)
      .csv(url_cities_file)
      .withColumn(colname_department_code, toInteger(col(colname_department_code)))
  }

  /**
    * retourne la moyenne des Departement d'une region
    * @param df
    * @return DataFrame
    */
  def avgDepByReg(df : DataFrame): DataFrame = {
    df.groupBy(col(colname_code_region), col(colname_nom_region))
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

package xke.local

import org.apache.avro.generic.GenericData.StringType
import org.apache.commons.lang.ObjectUtils.Null
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


object HelloWorld {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    val input = args(0)
    // code
    // src/main/resources/departements-france.csv
    // 1) lire le fichier
    // 2) créer une colonne avec la moyenne des numéro département par code région
    //    code_region, avg_dep, nom_region
    // 3) renommer la colonne moyenne des départements en avg_dep
    // 4) écrire le fichier en parquet
    val df = spark.read.option("sep",",").option("header",true  ).csv(input)
    val old = avgDepByReg(df)
    val _new = renameColumn(old)
    _new.write.mode("append").parquet("src/main/resources/departements-france-out.parquet")
  }

  def avgDepByReg(df: DataFrame)  = {
    df.withColumn("code_departement", stringToIntUdf(col("code_departement")))
      .groupBy(df("code_region"),df("nom_region"))
      .avg("code_departement")
  }
  def renameColumn(df: DataFrame): DataFrame = {
    df.withColumnRenamed("avg(code_departement)","avg_dep")
  }

  def stringToInt(input: String)  = input match {
    case x if x.startsWith("0")  => None
    case x if x.contains('A')=> None
    case x if x.contains('B')=> None
    case _  => Some(input.toInt);
  }

  val stringToIntUdf = udf( (x: String) => stringToInt(x))

  def joinLocations(villes: DataFrame, departements: DataFrame, regions: DataFrame): DataFrame = {
    villes
      .withColumnRenamed("name","city_name")
      .join(departements).where(col("department_code")=== col("code"))
      .withColumnRenamed("name","departement_name")
      .drop(col("code"))
      .join(regions).where(col("region_code")=== col("code"))
      .withColumnRenamed("name","region_name")
      .drop(col("code"))
  }

  def writeLocations(locations: DataFrame, output: String) = {
    locations.write.mode("append").partitionBy("region_code","departement_code").parquet(output)
  }

}

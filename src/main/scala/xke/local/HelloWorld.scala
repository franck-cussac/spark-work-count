package xke.local

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object HelloWorld {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate()
    //créer une UDF :
    // 1) qui prend un String en paramètre et renvoie un Int
    // 2) si votre String commence par un 0, on l'enlève
    // 3) si votre String contient un caractère non numérique, on l'enlève
    // 4) rédiger un test unitaire sur la fonction
    val toIntegerUdf: UserDefinedFunction = udf(parseInteger _)

    val df = spark.read.option("header", true).csv("src/main/resources/departements-france.csv")
      .withColumn("code_departement",toIntegerUdf(col("code_departement")))

    val dfCities = spark.read.option("header", true).csv("src/main/resources/cities.csv")

    val dfAvg = avgDepByReg(df = df)
    val dfRenameC = renameColumn(df = dfAvg)

    //df.write.mode(SaveMode.Overwrite).parquet("src/main/resources/parquet/ex1/")
    dfRenameC.write.mode("overwrite").parquet("src/main/ex1/parquet")

    dfRenameC.show
    val dfJoin = joinDf(df,dfCities)

    dfJoin.write.mode("overwrite")
      .partitionBy("code_region","code_departement")
      .parquet("src/main/parquet")

  }
  def parseInteger(s: String): Int = {
    s.filter(Character.isDigit).toInt
  }

  def avgDepByReg(df: DataFrame): DataFrame = {
    df.groupBy("code_region", "nom_region")
      .agg(avg("code_departement"))
  }

  def renameColumn(df: DataFrame): DataFrame = {
    df.withColumnRenamed("avg(code_departement)","avg_dep")
  }

  def joinDf(dfDepartement: DataFrame,dfCities: DataFrame): DataFrame = {
    dfDepartement.join(dfCities,dfDepartement("code_departement") === dfCities("department_code"))
  }
}
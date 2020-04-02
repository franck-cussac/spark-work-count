package xke.local

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object HelloWorld {

  val toIntegerUDF = udf(toInteger _)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate()
    //val spark = SparkSession.builder().getOrCreate()

    val dfDepartment = spark.read.option("delimiter", ",")
      .option("header", true)
      .csv("src/main/resources/departements-france.csv")

    val dfCities = spark.read.option("delimiter", ",")
      .option("header", true)
      .csv("src/main/resources/cities.csv")

    val df1Converted = dfDepartment.withColumn("code_departement", toIntegerUDF(col("code_departement")))
    val df2Converted = dfCities.withColumn("department_code", toIntegerUDF(col("department_code")))

    startWithAvg(df1Converted, "src/main/resources/avg_output.parquet")
    startWithJoin(df1Converted, df2Converted, "src/main/resources/join_output.parquet")
  }

  def startWithAvg(dataFrame: DataFrame, exportPath: String) : Unit = {
    val dfEnrich = dataFrameEnrichment(dataFrame)
    dfEnrich.write.mode(SaveMode.Overwrite).parquet(exportPath)
  }

  def startWithJoin(df1: DataFrame, df2: DataFrame, exportPath: String) : Unit = {
    val dfJoined = joinDataFrame(df1, df2)
    dfJoined.write.partitionBy("code_region", "code_departement").mode(SaveMode.Overwrite).parquet(exportPath)
  }

  def toInteger(value: String): Int = value.filter(Character.isDigit).toInt

  def joinDataFrame(dfDepartment: DataFrame, dfCities: DataFrame) : DataFrame = {
    dfDepartment.join(dfCities, dfDepartment("code_departement") === dfCities("department_code"), "inner")
  }

  def dataFrameEnrichment(dataFrame: DataFrame) : DataFrame = {
    val dfAvg = avgDepByReg(dataFrame)
    renameColumn(dfAvg)
  }

  def avgDepByReg(dataFrame: DataFrame) : DataFrame = {
    dataFrame.groupBy("code_region", "nom_region")
      .agg(avg("code_departement"))
      .select("code_region", "avg(code_departement)", "nom_region")
  }

  def renameColumn(dataFrame: DataFrame) : DataFrame = {
    dataFrame.withColumnRenamed("avg(code_departement)", "avg_dep")
  }
}

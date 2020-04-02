package xke.local

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object HelloWorld {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate()

    val parseToIntUdf: UserDefinedFunction = udf(parseToInt _)

    val df = spark.read
      .option("delimiter", ",")
      .option("header", true)
      .option("inferSchema", true)
      .csv("src/main/resources/departements-france.csv")
        .withColumn("code_departement", parseToIntUdf(col("code_departement")))
    df.show()

    val dfWithAvg = HelloWorld.avgDepByReg(df)
    dfWithAvg.show()

    val dfWithAvgAndName = HelloWorld.renameColumn(dfWithAvg, "avg_dep")
    dfWithAvgAndName.show()
  }

  def parseToInt(s: String): Int =
    s.filter(Character.isDigit).toInt

  def avgDepByReg(df: DataFrame): DataFrame =
    df.groupBy("code_region", "nom_region")
      .agg(avg("code_departement"))

  def renameColumn(df: DataFrame, newName: String): DataFrame =
    df.withColumnRenamed("avg(code_departement)", newName)
}

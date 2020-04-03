package xke.local

import com.sun.org.apache.xalan.internal.xsltc.compiler.util.IntType
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

import scala.reflect.io.Path


object HelloWorld {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("").getOrCreate()

    var path = "src/main/resources/departements-france.csv"
    var path2 = "src/main/resources/cities.csv"
    if(args.length > 0) {
      path = args(0)
    }

    val df1 = spark.read.option("sep", ",").option("header", true).csv(path)
    val df2 = spark.read.option("sep", ",").option("header", true).csv(path2)

    val exo1 = ex1(spark, df1)
    ex2(spark, df2, exo1)
  }

  def ex1(spark: SparkSession, df: DataFrame): DataFrame = {
    val dfIntColumn = df.withColumn("code_departement", col("code_departement").cast(IntegerType))
    val newDF = avgDepByReg(dataFrame = dfIntColumn)
    val newDfName = renameColumn(dataFrame = newDF)

    writeToParquet(dataFrame = newDfName, pathToFile = "ParquetResult")
    printParquet(spark, pathToFile = "ParquetResult")

    dfIntColumn
  }

  def ex2(spark: SparkSession, df: DataFrame ,ex1: DataFrame): Unit = {
    val newdf = df.withColumnRenamed("department_code", "code_departement")
    val join = joinUDF(newdf, ex1, "code_departement")
    join.show()
    writeJoinAndPartition(join, "code_region", "code_departement","ParquetResult2")
  }

  def avgDepByReg(dataFrame: DataFrame): DataFrame = {
    dataFrame.groupBy(col("code_region"), col("nom_region")).avg("code_departement").as("avg_dep")
  }

  def renameColumn(dataFrame: DataFrame): DataFrame = {
    dataFrame.withColumnRenamed("avg(code_departement)","avg_dep")
  }

  def writeToParquet(dataFrame: DataFrame, pathToFile: String): Unit = {
    dataFrame.write.mode(SaveMode.Overwrite).parquet(pathToFile)
  }

  def printParquet(spark: SparkSession, pathToFile: String) {
    spark.read.parquet(pathToFile).show
  }

  def string2Int(value: String): Int = {
    value.filter(Character.isDigit).toInt
  }

  val stringToIntUdf: UserDefinedFunction = udf(string2Int _ )

  def writeJoinAndPartition(df: DataFrame, col1: String, col2: String, path: String): Unit = {
    df.write.partitionBy(col1, col2).mode(SaveMode.Overwrite).parquet(path)
  }

  def joinUDF(df: DataFrame, ex1: DataFrame, colName: String): DataFrame = {
    df.withColumn(colName,df(colName)).as(colName)
      .join(ex1.withColumn(colName, ex1(colName)).as(colName), colName)
  }

}
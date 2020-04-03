package xke.local

import com.sun.org.apache.xalan.internal.xsltc.compiler.util.IntType
import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object HelloWorld {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("").getOrCreate()

    val df = readCSVFile(spark, "src/main/resources/departements-france.csv")
    val dfIntColumn = df.withColumn("code_departement", col("code_departement").cast(IntegerType))
    val newDF = avgDepByReg(dataFrame = dfIntColumn)
    val newDfColumn = newColumn(dataFrame = newDF, columnName = "average", value = lit(""))
    val newDfName = renameColumn(dataFrame = newDfColumn, columnName = "avg(code_departement)", newName = "avg_dep")

    writeParquet(dataFrame = newDfName, "ParquetResult")
    showParquet(spark ,"ParquetResult")
  }

  val stringToIntUdf: UserDefinedFunction = udf(stringToInt _ )

  def avgDepByReg(dataFrame: DataFrame): DataFrame = {
    dataFrame.groupBy(col("code_region")).avg("code_departement").as("avg_dep")
  }

  def renameColumn(dataFrame: DataFrame, columnName: String, newName: String): DataFrame = {
    dataFrame.withColumnRenamed(columnName,newName)
  }

  def newColumn(dataFrame: DataFrame, columnName: String, value: Column): DataFrame = {
    dataFrame.withColumn(columnName, value)
  }

  def writeParquet(dataFrame: DataFrame, outputName: String): DataFrame = {
    dataFrame.write.mode(SaveMode.Overwrite).parquet(outputName)
    dataFrame
  }

  def readCSVFile(spark: SparkSession, pathToFile: String): DataFrame = {
    spark.read.option("delimiter", ",").option("header", true).csv(pathToFile)
  }

  def showParquet(spark: SparkSession, pathToFile: String){
    spark.read.parquet(pathToFile).show
  }

  def stringToInt(str: String): Int = {
    str.filter(Character.isDigit).toInt
  }

  

}

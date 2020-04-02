package xke.local

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object HelloWorld {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]")getOrCreate() // ctrl + Q affiche le type de la variable4

//    val df_departement = createDateFrameDepartement(spark)
//    val df_cities = createDateFrameCity(spark);
//    val avg_dep_df = avgDepByReg(df_departement)
//    val result_df = renameColumn(avg_dep_df)

//    result_df.write.mode(SaveMode.Overwrite).parquet("/home/ubuntu/workspace/hadoop/spark-work-count/src/main/resources/parquet")

    val stringToIntUdf: UserDefinedFunction = udf(stringToInt _ )

    val df_departements = spark.read.option("header", true).csv("src/main/resources/departements-france.csv")
      .withColumn("code_departement", stringToIntUdf(col("code_departement")))

    val df_cities = spark.read.option("header", true).csv("src/main/resources/cities.csv")
      .withColumn("department_code", stringToIntUdf(col("department_code")))
      .withColumnRenamed("department_code", "code_departement")
      .withColumnRenamed("name", "nom_ville")

    val df_joined = df_departements.join(df_cities, Seq("code_departement"), "left")

    df_joined.show(100)
  }



  def stringToInt(str: String): Int = {
    str.filter(Character.isDigit).toInt
  }

  def createDateFrameDepartement(sparkSession: SparkSession): DataFrame = {
    val stringToIntUdf: UserDefinedFunction = udf(stringToInt _ )
    sparkSession.read.option("header", true)
      .csv("src/main/resources/departements-france.csv")
      .withColumn("code_departement", stringToIntUdf(col("code_departement")))
  }

  def createDateFrameCity(sparkSession: SparkSession): DataFrame = {
    val stringToIntUdf: UserDefinedFunction = udf(stringToInt _ )
    sparkSession.read.option("header", true)
      .csv("src/main/resources/cities.csv")
      .withColumn("department_code", stringToIntUdf(col("department_code")))
      .withColumnRenamed("department_code", "code_departement")
  }

  def avgDepByReg(dataFrame: DataFrame): DataFrame = {
    dataFrame.groupBy("code_region", "nom_region")
      .agg(avg("code_departement"))
  }

  def renameColumn(dataFrame: DataFrame): DataFrame = {
    dataFrame.withColumnRenamed("avg(code_departement)", "avg_dep")
  }

}

package xke.local

import io.netty.handler.codec.spdy.DefaultSpdyDataFrame
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object HelloWorld {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Mahdi").master("local[*]").getOrCreate()

    // code
    // src/main/resources/departements-france.csv
    // 1) lire le fichier
    // 2) créer une colonne avec la moyenne des numéro département par code région
    //    code_region, avg_dep, nom_region
    // 3) renommer la colonne moyenne des départements en avg_dep
    // 4) écrire le fichier en parquet
    if(args.length >= 2) {
      val result = readDataAndApplyTransformations(spark, args(0))
      result.write.mode(SaveMode.Overwrite).parquet(args(1))
      result.show()
    }else {
      val result = readDataAndApplyTransformations(spark, "src/main/resources/departements-france.csv")
      result.write.mode(SaveMode.Overwrite).parquet("result")
      result.show()
    }

  }

  private def readDataAndApplyTransformations(spark: SparkSession, dataPath: String): DataFrame = {
    val df = spark.read.option("delimiter", ",").option("header", value = true).option("inferSchema", "true").csv(dataPath)
    val intermediate = avgDepByReg(df)
    val result = renameColumn(intermediate, "avg(code_departement)", "avg_dep")
    result;
  }

  def avgDepByReg: DataFrame => DataFrame =
    (df: DataFrame) =>  df.groupBy("code_region", "nom_region").agg(avg("code_departement"))

  def renameColumn(dataFrame: DataFrame, oldName: String, newName:String): DataFrame = {
    dataFrame.withColumnRenamed(oldName, newName)
  }
}

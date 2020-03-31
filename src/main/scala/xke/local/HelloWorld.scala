package xke.local

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object HelloWorld {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate()
    import spark.implicits._

    // code
    // src/main/resources/departements-france.csv
    // 1) lire le fichier
    // 2) créer une colonne avec la moyenne des numéro département par code région
    //    code_region, avg_dep, nom_region
    // 3) renommer la colonne moyenne des départements en avg_dep
    // 4) écrire le fichier en parquet

    if (args.length == 0) {
      val df = spark.read.option("header", true).csv("src/main/resources/departements-france.csv")
        .withColumn("code_departement", col("code_departement").cast("integer"))

      writeParquet(
        renameColumn(
          avgDepByReg(df)
        )
      )
    }
    else {
      val df = spark.sparkContext.parallelize(List(
        (1, 2, "toto"),
        (1, 3, "toto"),
        (1, 4, "toto"),
        (2, 14, "zaza"),
        (2, 54, "zaza"),
        (2, 7, "zaza"),
        (2, 5, "zaza")
      )).toDF("code_region", "code_departement", "nom_region")
      writeParquet(
        renameColumn(
          avgDepByReg(df)
        )
      )
    }
  }

  def avgDepByReg(df: DataFrame): DataFrame = {
    df.groupBy("code_region", "nom_region")
      .agg(avg("code_departement"))
  }
  def renameColumn(df: DataFrame): DataFrame = {
    df.withColumnRenamed("avg(code_departement)", "avg_dep")
  }
  def writeParquet(df: DataFrame): Unit = {
    df.write.mode("overwrite").parquet("src/main/parquets/output.parquet")
  }
}

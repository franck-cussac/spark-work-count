package xke.local

import java.io.File

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf

import scala.reflect.io.Directory

object MainExo {
    def main(args: Array[String]): Unit = {
        exo_avgDep()
        //exo_udf()
    }

    def exo_avgDep() = {
        // code
        // src/main/resources/departements-france.csv
        // 1) lire le fichier
        // 2) créer une colonne avec la moyenne des numéro département par code région
        //    code_region, avg_dep, nom_region
        // 3) renommer la colonne moyenne des départements en avg_dep
        // 4) écrire le fichier en parquet

        new Directory(new File("./avgdep-parquet")).deleteRecursively()

        val spark = SparkSession
            .builder()
            .appName("test")
            .master("local[*]")
            .getOrCreate()

        var df = spark
            .read
            .option("delimiter", ",")
            .option("header", true)
            .csv("src/main/resources/departements-france.csv")

        df = avgDepByReg(df)
        df = renameColumn(df)

        df.show()
        df.write.parquet("avgdep-parquet")
    }

    def exo_udf() = {
        val spark = SparkSession
            .builder()
            .appName("test")
            .master("local[*]")
            .getOrCreate()

        import spark.implicits._

        val df = spark.read
            .option("delimiter", ",")
            .option("header", true)
            .csv("src/main/resources/departements-france.csv")

        val removeZeroAndToInt = udf(_removeZeroAndToInt _)

        df
            .withColumn("code_departement", removeZeroAndToInt($"code_departement"))
            .show(50)
    }

    def _removeZeroAndToInt(s: String): Int = {
        var newS: String = s

        if (newS.charAt(0) == '0') {
            newS = newS.substring(1)
        }

        if (newS.contains("A") || newS.contains("B")) {
            newS = newS.replaceAll("A", "")
            newS = newS.replaceAll("B", "")
        }

        newS.toInt
    }

    def avgDepByReg(df: DataFrame): DataFrame = {
        df
            .withColumn("code_departement", col("code_departement"))
            .groupBy("code_region", "nom_region")
            .agg(
                avg("code_departement")
            )
    }

    def renameColumn(df: DataFrame): DataFrame = {
        df
            .withColumnRenamed("avg(code_departement)", "avg_dep")
    }
}
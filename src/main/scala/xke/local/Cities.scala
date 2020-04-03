package xke.local

import java.io.File

import org.apache.spark.sql.SparkSession

import scala.reflect.io.Directory

object Cities {
    def main(args: Array[String]): Unit = {
        new Directory(new File(("./cities-parquet"))).deleteRecursively()

        val spark = SparkSession.builder().appName("cities").master("local[*]").getOrCreate()

        var regions = spark.read.option("delimiter", ",").option("header", true).csv("src/main/resources/regions.csv")
        regions = regions.withColumnRenamed("code", "p_region_code")
        regions = regions.withColumnRenamed("name", "region_name")
        regions = regions.select("p_region_code", "region_name")

        var departments = spark.read.option("delimiter", ",").option("header", true).csv("src/main/resources/departments.csv")
        departments = departments.withColumnRenamed("code", "p_department_code")
        departments = departments.withColumnRenamed("name", "department_name")
        departments = departments.select("p_department_code", "department_name", "region_code")

        var cities = spark.read.option("delimiter", ",").option("header", true).csv("src/main/resources/cities.csv")

        departments = departments.join(
            regions,
            departments("region_code") === regions("p_region_code"),
            "left_outer")

        cities = cities.join(
            departments,
            cities("department_code") === departments("p_department_code"),
            "left_outer"
        )

        cities = cities
            .select(
                "id","name",
                "department_name", "region_name",
                "zip_code", "insee_code",
                "gps_lat", "gps_lng",
                "p_region_code", "p_department_code")

        cities.write.partitionBy("p_region_code", "p_department_code").parquet("cities-parquet")
    }
}

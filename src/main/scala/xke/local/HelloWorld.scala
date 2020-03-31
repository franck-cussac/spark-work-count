package xke.local

import org.apache.spark.sql.{SaveMode, SparkSession}

object HelloWorld {
  def main(args: Array[String]): Unit = {
    /*val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val inputFile = args(0)
    val outputFile = args(1)
    val input =  spark.sparkContext.textFile(inputFile)
    val words = input.flatMap(line => line.split(" "))
    val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
    counts.toDF("word", "count").write.mode(SaveMode.Overwrite).parquet(outputFile)*/

    val spark = SparkSession.builder().master("local[*]").appName("test").getOrCreate()
    //src/main/resources/departements-france.csv
    //1 lire
    //2 code region paire
    //3 regrouper région par dizaine : 1 ligne 20aine avec liste région
    //4 garder ligne avec 10 résultats
    //5 afficher nb ligne final

    /*val df = spark.read.option("delimiter", ",").option("header", true).csv("src/main/resources/departements-france.csv")
    df.show()
    val res = df.filter(df("code_departement") % 2 === 0)
    res.show();
    println(df.count())*/
  }
}

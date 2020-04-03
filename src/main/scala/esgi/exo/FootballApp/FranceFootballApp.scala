package esgi.exo.FootballApp

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._



object FranceFootballApp {

  val DF_MATCH_PATH = "src/main/resources/df_matches.csv"
  val FILTERING_DATE = "1980-03-01"
  val PARQUET_PATH_STATS = "src/main/resources/stat.parque"
  val PARQUET_PATH_RESULT = "src/main/resources/result.parque"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("FranceFootballApp").master("local[*]").getOrCreate()
    import spark.implicits._

    val dfMatch = spark.read
      .option("delimiter", ",")
      .option("header", value = true)
      .option("inferSchema", "true")
      .csv(DF_MATCH_PATH)


    /////------Part 1------////
    //Rename column name
    val dfMatchWithColumnRenamed = renameDfColumns(dfMatch)

    //Convert column date to DateType
    val dfMatchWithDateType = dfMatchWithColumnRenamed
      .withColumn("date", to_date(col("date"), "yy-MM-dd"))

    // Replace null value on penalty column with 0
    val dfMatchWithoutNull = dfMatchWithDateType
      .withColumn("penalty_france",replaceNAWithZeroUDF($"penalty_france"))
      .withColumn("penalty_adversaire",replaceNAWithZeroUDF($"penalty_adversaire"))

    // Filter after mars 1980
    val dfMatchFilteredWithDate =  filterMatchAfterGivenDate(dfMatchWithoutNull, FILTERING_DATE)

    // Select only columns that interest us
    val columnNames = Seq("match", "competition", "adversaire", "score_france", "score_adversaire", "penalty_france", "penalty_adversaire", "date")
    val dfResultPart1 = dfMatchFilteredWithDate
      .select(columnNames.map(c => col(c)): _*)

    /////------Part 2------////

    // Add is_home_column
    val dfMatchWithIsHomeMatch = dfResultPart1
      .withColumn("match_a_domicile", isHomeMatchUDF(col("match")))

    // Calculate stats and write parquet
    val dfResultStats = calculateStats(dfMatchWithIsHomeMatch)
    dfResultStats.write.mode("overwrite").parquet(PARQUET_PATH_STATS)

    /////------Part 3------////
    joinDFResultPart1AndDfStats(
      dfResultPart1,
      dfResultStats.withColumnRenamed("adversaire", "adversaire_bis")
    ).show()
  }

  def filterMatchAfterGivenDate(df: DataFrame, date: String): DataFrame = {
    df.filter(col("date").gt(lit(date)))
  }

  def renameDfColumns(df: DataFrame): DataFrame = {
    df.withColumnRenamed("X4", "match")
      .withColumnRenamed("X6", "competition")
  }

  // UDF to change NA value in columns with 0
  val replaceNAWithZero: String => Int = (value: String) => if(value != "NA")  value.toInt else 0
  val replaceNAWithZeroUDF: UserDefinedFunction = udf(replaceNAWithZero)

  //UDF to know if is France home match or not
  val isHomeMatch: String => Boolean = (input: String) => input.indexOf("France") == 0
  val isHomeMatchUDF: UserDefinedFunction = udf(isHomeMatch)

  //Calculate stats
  def calculateStats(dfMatchWithIsHomeMatch: DataFrame): DataFrame = {
    dfMatchWithIsHomeMatch.groupBy("adversaire")
      .agg(
        avg("score_france").as("avg_score_france"),                 //nb points France
        avg("score_adversaire").as("avg_score_adversaire"),         //nb points Adversaire
        count("match").as("total_nb_match"), // Total match
        count(when(col("match_a_domicile"), true)).as("total_a_domicile"),
        count(
          when(col("competition").startsWith("Coupe du monde"),               //nb match coupe de monde
            col("competition"))
        ).as("total_nb_coupe_de_monde"),
        max("penalty_adversaire").as("max_penalty_reçu_france"),    //max pinalté reçu France
        sum("penalty_france").as("total_penalty_france"),
        sum("penalty_adversaire").as("total_penalty_adversaire")
      )
      .withColumn(
        "pourcentage_a_domicile",
        col("total_a_domicile") * 100 / col("total_nb_match")
      )
      .withColumn("difference_pinalty", col("total_penalty_adversaire") - col("total_penalty_france"))
      // Delete unused columns.
      .drop("total_penalty_france")
      .drop("total_penalty_adversaire")
      .drop("total_a_domicile")
  }

  def joinDFResultPart1AndDfStats(dfPart1: DataFrame, dfStats: DataFrame): DataFrame = {
    dfPart1
      .join(dfStats, dfPart1("adversaire") === dfStats("adversaire_bis"), "left_outer")
      .drop("adversaire_bis")
  }
}

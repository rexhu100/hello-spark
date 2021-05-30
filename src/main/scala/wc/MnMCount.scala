package wc

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object MnMCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("M&M Counter").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    if (args.length < 1) {
      println("Usage: MnMCount <mnm_file_dataset>")
      sys.exit(1)
    }

    val dataFile = args(0)

    val df: DataFrame = spark.read.format("csv").option("header", true).option("inferSchema", true).load(dataFile)
    println("Showing a few rows of the data.")
    df.show(10)

    val countDF = df.select("State", "Color", "Count")
      .groupBy("State", "Color")
      .agg(count("Count").alias("ct"), sum("Count").alias("s"))
      .orderBy(desc("ct"))

    countDF.show(10)
    println(s"Total rows = ${countDF.count()}")

    val tempCountDF = countDF.where(col("State") === "TX")

    println("Showing the aggregated data for Texas")
    tempCountDF.show(20, false)

    spark.stop()
  }
}

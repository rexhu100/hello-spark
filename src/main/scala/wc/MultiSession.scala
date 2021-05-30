package wc

import org.apache.spark.sql.SparkSession

object MultiSession {
  def main(args: Array[String]): Unit = {
    val spark1 = SparkSession.builder().master("local[2]").appName("M&M Counter Uno").getOrCreate()
    spark1.sparkContext.setLogLevel("WARN")

    val spark3 = spark1.newSession()

    println(spark1.toString())
    println(spark3.toString())
  }
}

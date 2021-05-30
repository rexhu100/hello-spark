package wc

import org.apache.spark.sql.SparkSession

object MultiSession {
  def main(args: Array[String]): Unit = {
    val spark1 = SparkSession.builder().master("local[2]").appName("M&M Counter Uno").getOrCreate()
    val spark2 = SparkSession.builder().master("local[2]").appName("M&M Counter Duo").getOrCreate()
    spark1.sparkContext.setLogLevel("WARN")

    val spark3 = spark1.newSession()

    // The point is to show that we can create multiple SparkSession in one application. However we cannot do so by
    // multiple calls to getOrCreate. Instead, we can achieve this by using the newSession() method.
    println(spark1.toString)
    println(spark2 toString)
    println(spark3.toString)
  }
}

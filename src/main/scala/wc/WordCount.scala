package wc

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object WordCount {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Usage: WordCount <text_file>")
      sys.exit(1)
    }

    val fileName = args(0)

    val spark = SparkSession.builder().appName("Word Counter").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val textData: RDD[String] = spark.sparkContext.textFile(fileName)
    val counts = textData.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)

    counts.foreach(println)

    spark.close()
  }
}

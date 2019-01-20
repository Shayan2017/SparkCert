package com.sundogsoftware.spark.section3

import org.apache.spark.SparkContext

object MovieRatings {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]", "MovieRatings")

    val lines = sc.textFile("src/main/resources/ml-100k/sample.txt")
    val rdd = lines.map(parse)
    /*
        val movieRatings = rdd.mapValues(r => (r, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
        val averages = movieRatings.mapValues(x => x._1 / x._2)
        val result = averages.collect()

        // Sort and print the final results.
        result.sorted.foreach(println)
    */
    val sumRDD = rdd.map(v => ((v._1, v._2), v._3)).reduceByKey(_ + _)
    val results = sumRDD.map(v => (v._1._1, (v._1._2, v._2))).groupByKey()
    results.foreach(println)
  }

  def parse(line: String): (Int, Int, Int) = {
    val fields = line.split("\t")
    val movieId = fields(1).toInt
    val rating = fields(2).toInt

    (movieId, rating, 1)
  }
}

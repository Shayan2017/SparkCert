package com.sundogsoftware.spark.section3

import org.apache.log4j._
import org.apache.spark._

object CustomerOrders {

  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "CustomerOrders")

    // Read each line of my book into an RDD
    val input = sc.textFile("src/main/resources/customer-orders.csv")

    // Split into words separated by a space character
    val results = input.map(parseLine)
      .reduceByKey((v1, v2) => v1 + v2)
      .map(x => (x._2, x._1)) // => sorting by amount spent
      .collect()

    for (result <- results.sorted) {
      val custId = result._2
      val total = result._1
      println(s"$total spent by $custId")
    }

  }

  def parseLine(line: String) = {
    val fields = line.split(",")
    val customerId = fields(0).toInt
    val amount = fields(2).toFloat
    (customerId, amount)
  }
}
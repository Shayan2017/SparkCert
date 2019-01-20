package com.sundogsoftware.spark.section3

import org.apache.log4j._
import org.apache.spark._

import scala.math.max

/** Find the maximum temperature by weather station for a year */
object PrecipitationTemperatures {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "PrecipitationTemperatures")

    val lines = sc.textFile("resources/1800.csv")
    val parsedLines = lines.map(parseLine)
    val maxTemps = parsedLines.filter(x => x._3 == "PRCP")
    println("maxTemps: " + maxTemps)

    val stationTemps = maxTemps.map(x => (x._2, x._4.toFloat))
    println("stationTemps: " + stationTemps)

    val maxTempsByStation = stationTemps.reduceByKey((x, y) => max(x, y))
    println("maxTempsByStation: " + maxTempsByStation)

    val results = maxTempsByStation.collect()

    for (result <- results.sorted) {
      val station = result._1
      //val day = result._1
      val temp = result._2
      val formattedTemp = f"$temp%.2f F"
      println(s"$station max temperature: $formattedTemp on day ")
    }

  }

  def parseLine(line: String) = {
    val fields = line.split(",")
    val stationID = fields(0)
    val day = fields(1)
    val entryType = fields(2)
    val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (stationID, day, entryType, temperature)
  }
}
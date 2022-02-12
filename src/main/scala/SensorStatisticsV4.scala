package com.luxoft

import models.SensorRankingEntry

import java.io.{File, FileInputStream}
import scala.collection.mutable
import scala.io.{BufferedSource, Source}

object SensorStatisticsV4 {
  def main(arguments: Array[String]): Unit = {
    val files: List[File] = new File(arguments.head).listFiles.filter(_.isFile).toList

    var filesProcessed: Int = 0
    var numberOfProcessedMeasurements: Long = 0L
    var numberOfFailedMeasurements: Long = 0L
    val sensorReadingsCountMap: mutable.Map[String, Long] = mutable.Map.empty
    val sensorRankingEntries: mutable.Map[String, SensorRankingEntry] = mutable.Map.empty

    files.foreach { f =>
      filesProcessed += 1

      val fileInputStream: FileInputStream = new FileInputStream(f)
      val bufferedSource: BufferedSource = Source.createBufferedSource(fileInputStream, 1024)
      val linesFromBuffer: Iterator[String] = bufferedSource.getLines().drop(1)

      for(line <- linesFromBuffer) {
        val columns: Array[String] = line.split(",").map(_.trim)
        val sensorId: String = columns(0)

        if(!columns(1).contains("NaN")) {
          val reading: Int = columns(1).toInt

          numberOfProcessedMeasurements += 1

          if(sensorReadingsCountMap.contains(sensorId)) {
            val min: Int = sensorRankingEntries(sensorId).min
            val max: Int = sensorRankingEntries(sensorId).max

            sensorReadingsCountMap(sensorId) += 1

            if(reading < min) sensorRankingEntries(sensorId) = SensorRankingEntry(reading, (reading + max) / 2, max)
            else if(reading > max) sensorRankingEntries(sensorId) = SensorRankingEntry(min, (min + reading) / 2, reading)
            else sensorRankingEntries(sensorId) = SensorRankingEntry(min, (min + max) / 2, max)
          } else {
            sensorReadingsCountMap addOne(sensorId, 1L)
            sensorRankingEntries addOne(sensorId, SensorRankingEntry(reading, reading, reading))
          }
        } else numberOfFailedMeasurements += 1
      }

      bufferedSource.close()
    }

    println(s"Num of processed files: $filesProcessed")
    println(s"Num of processed measurements: $numberOfProcessedMeasurements")
    println(s"Num of failed measurements: $numberOfFailedMeasurements")
    println("\nSensors with highest avg humidity:\n")
    println("sensor-id,min,avg,max")
    println("---------------------")

    sensorRankingEntries.toList.sortWith((left, right) => left._2.avg > right._2.avg)
      .foreach(sre => println(sre._1 + "," + sre._2.min + "," + sre._2.avg + "," + sre._2.max))
  }
}
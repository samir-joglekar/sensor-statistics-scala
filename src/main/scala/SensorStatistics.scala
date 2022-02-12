package com.luxoft

import models.{SensorRankingEntryOld, SensorReading}

import cats.effect.{ExitCode, IO, IOApp}
import fs2.io.file.{Files, Flags, Path}
import fs2.{Pipe, text}

import java.io.File
import scala.util.Try

object SensorStatistics extends IOApp {
  def convertSensorReadingLine: List[String] => Option[SensorReading] = {
    case sensorId :: humidity:: Nil => Try(SensorReading(sensorId = sensorId, humidity = Try(humidity.toInt) getOrElse(-1))).toOption
    case _ => None
  }

  def csvParser[F[_]]: Pipe[F, Byte, List[String]] = _.through(text.utf8.decode).through(text.lines).drop(1).map(_.split(',').toList)

  def parseConvert(directory: String): List[SensorRankingEntryOld] = {
    val files: List[File] = new File(directory).listFiles.filter(_.isFile).toList
    var filesProcessed = 0
    var numberOfProcessedMeasurements = 0
    var numberOfFailedMeasurements = 0
    var sensorRankingEntryList: List[SensorRankingEntryOld] = List[SensorRankingEntryOld]()

    files.foreach { f =>
      filesProcessed += 1

      var sensorRankingEntry: SensorRankingEntryOld = null

      Files[IO].readAll(path = Path(f.getPath), chunkSize = 1024, flags = Flags.Read).through(csvParser).map(convertSensorReadingLine)
        .unNoneTerminate
        .evalMap(r => IO({
          if(r.humidity != -1) numberOfProcessedMeasurements += 1 else numberOfFailedMeasurements += 1

          if(sensorRankingEntryList.contains(r.sensorId)) {
            val min: Int = sensorRankingEntryList.filter(_.sensorId.equals(r.sensorId)).head.min
            val max: Int = sensorRankingEntryList.filter(_.sensorId.equals(r.sensorId)).head.max

            if(r.humidity > max) sensorRankingEntry = SensorRankingEntryOld(r.sensorId, min, (min + r.humidity) / 2, r.humidity)
            else if(r.humidity < min) sensorRankingEntry = SensorRankingEntryOld(r.sensorId, r.humidity, (r.humidity + max) / 2, max)
            else sensorRankingEntry = SensorRankingEntryOld(r.sensorId, min, (min + max) / 2, max)
          } else sensorRankingEntry = SensorRankingEntryOld(r.sensorId, r.humidity, r.humidity, r.humidity)

          sensorRankingEntry
        })).head

      sensorRankingEntryList ::= sensorRankingEntry
    }

    println(s"Num of processed files: $filesProcessed")
    println(s"Num of processed measurements: $numberOfProcessedMeasurements")
    println(s"Num of failed measurements: $numberOfFailedMeasurements")

    sensorRankingEntryList.sortWith((left, right) => left.avg > right.avg)
  }

  def parseConvertSingleFile(sourceFile: String, sensorRankingEntriesFromLast: List[SensorRankingEntryOld]): List[SensorRankingEntryOld] = {
    var numberOfProcessedMeasurements = 0
    var numberOfFailedMeasurements = 0
    var sensorRankingEntryList: List[SensorRankingEntryOld] = sensorRankingEntriesFromLast

    var sensorRankingEntry: SensorRankingEntryOld = null

    Files[IO].readAll(path = Path(sourceFile), chunkSize = 1024, flags = Flags.Read).through(csvParser).map(convertSensorReadingLine)
      .unNoneTerminate
      .evalMap(r => IO({
        if(r.humidity != -1) numberOfProcessedMeasurements += 1 else numberOfFailedMeasurements += 1

        if(sensorRankingEntryList.contains(r.sensorId)) {
          val min: Int = sensorRankingEntryList.filter(_.sensorId.equals(r.sensorId)).head.min
          val max: Int = sensorRankingEntryList.filter(_.sensorId.equals(r.sensorId)).head.max

          if(r.humidity > max) sensorRankingEntry = SensorRankingEntryOld(r.sensorId, min, (min + r.humidity) / 2, r.humidity)
          else if(r.humidity < min) sensorRankingEntry = SensorRankingEntryOld(r.sensorId, r.humidity, (r.humidity + max) / 2, max)
          else sensorRankingEntry = SensorRankingEntryOld(r.sensorId, min, (min + max) / 2, max)
        } else sensorRankingEntry = SensorRankingEntryOld(r.sensorId, r.humidity, r.humidity, r.humidity)

        sensorRankingEntry
      }))

    sensorRankingEntryList ::= sensorRankingEntry

    sensorRankingEntryList.sortWith((left, right) => left.avg > right.avg)
  }

  override def run(args: List[String]): IO[ExitCode] = {
    val files: List[File] = new File(args.head).listFiles.filter(_.isFile).toList
    var sensorRankings: List[SensorRankingEntryOld] = Nil

    files.foreach { f =>
      sensorRankings = parseConvertSingleFile(f.getPath, sensorRankings)
    }

    println(sensorRankings)
//    val sensorRankings: List[SensorRankingEntry] = parseConvert(args.head)

//    println("\nSensors with highest avg humidity:\n")
//    println("sensor-id,min,avg,max")

//    sensorRankings.foreach(sr => println(sr.sensorId + "," + sr.min + "," + sr.avg + "," + sr.max))

    IO(ExitCode.Success)
  }
}
package com.luxoft

import models.{SensorRankingEntry, SensorRankingEntryOld, SensorReading}

import cats.effect.{ExitCode, IO, IOApp}
import fs2.io.file.{Files, Flags, Path}
import fs2.{Pipe, Stream, text}

import java.io.File
import scala.util.Try

object SensorStatisticsV3 extends IOApp {
  def convertSensorReadingLine: List[String] => Option[SensorReading] = {
    case sensorId :: humidity:: Nil => Try(SensorReading(sensorId = sensorId, humidity = Try(humidity.toInt) getOrElse(-1))).toOption
    case _ => None
  }

  def csvParser[F[_]]: Pipe[F, Byte, List[String]] = _.through(text.utf8.decode).through(text.lines).drop(1).map(_.split(',').toList)

  def processReading(fileName: String, reading: Option[SensorReading], existingRankings: List[SensorRankingEntry]): List[SensorRankingEntry] = {
    println(fileName)
    println(reading.get)
    existingRankings :+ SensorRankingEntryOld(reading.get.sensorId, 1, 1, 1)
    existingRankings
  }

  def parser(filePath: String, sensorRankingEntries: List[SensorRankingEntry]): Stream[IO, List[SensorRankingEntry]] =
    Files[IO].readAll(path = Path(filePath), chunkSize = 1024, flags = Flags.Read).through(csvParser).map(convertSensorReadingLine)
      .evalMap(x => IO[List[SensorRankingEntry]](processReading(filePath, x, sensorRankingEntries)))

  override def run(args: List[String]): IO[ExitCode] = {
    val files: List[File] = new File(args.head).listFiles.filter(_.isFile).toList

    var filesProcessed: Int = 0
    val existingRankings: List[SensorRankingEntry] = List.empty

    val a: List[List[SensorRankingEntry]] = files.map(f => {
      filesProcessed += 1
      parser(f.getPath, existingRankings).compile.drain
      List.empty
    })

    println(a)

    IO(ExitCode.Success)
  }
}
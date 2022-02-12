package com.luxoft

import models.SensorReading

import cats.effect.{ExitCode, IO, IOApp}
import fs2.io.file.{Files, Flags, Path}
import fs2.{Pipe, Stream, text}

import java.io.File
import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}
import scala.util.Try

object SensorStatisticsV2 extends IOApp {
  val blockingExecutionContext: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(2))

  def convertSensorReadingLine: List[String] => Option[SensorReading] = {
    case sensorId :: humidity:: Nil => Try(SensorReading(sensorId = sensorId, humidity = Try(humidity.toInt) getOrElse(-1))).toOption
    case _ => None
  }

  def csvParser[F[_]]: Pipe[F, Byte, List[String]] = _.through(text.utf8.decode).through(text.lines).drop(1).map(_.split(',').toList)

  def parserV2(directoryPath: String): Stream[IO, Unit] = {
    val files: List[File] = new File(directoryPath).listFiles.filter(_.isFile).toList

    for(f <- files) {
      Files[IO].readAll(path = Path(f.getPath), chunkSize = 1024, flags = Flags.Read).through(csvParser)
        .map(convertSensorReadingLine).map(x => IO(processReading(f.getPath, x)))
    }

    Stream.empty
  }

  def parser(filePath: String): Stream[IO, Unit] = Files[IO].readAll(path = Path(filePath), chunkSize = 1024,
    flags = Flags.Read).through(csvParser).map(convertSensorReadingLine).evalMap(x => IO(processReading(filePath, x)))

  def processReading(fileName: String, reading: Any): Unit = {
    println(fileName)
    println(reading)
  }

  def parseAndPrint(filePath: String): Stream[IO, Unit] = {
    Files[IO].readAll(path = Path(filePath), chunkSize = 1024, flags = Flags.Read).through(csvParser).map(convertSensorReadingLine)
      .evalMap(x => IO(processReading(filePath, x)))
  }

  override def run(args: List[String]): IO[ExitCode] = {
    parserV2(directoryPath = "/site/sensor-statistics/sensor-data")

    IO(ExitCode.Success)
  }
}
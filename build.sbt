ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

lazy val root: Project = (project in file(".")).settings(name := "sensor-statistics-scala", idePackagePrefix := Some("com.luxoft"))

libraryDependencies += "co.fs2" %% "fs2-core" % "3.2.4"
libraryDependencies += "co.fs2" %% "fs2-io" % "3.2.4"
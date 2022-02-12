package com.luxoft
package models

case class SensorRankingEntry(min: Int, avg: Int, max: Int)

case class SensorRankingEntryOld(sensorId: String, min: Int, avg: Int, max: Int)
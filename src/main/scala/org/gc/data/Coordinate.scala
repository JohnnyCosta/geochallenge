package org.gc.data

/**
  * Coordinate with ID
  */
class Coordinate(val id: String, val lat: Double, val lon: Double)

object Coordinate {
  def apply(id: String, lat: Double, lon: Double): Coordinate = new Coordinate(id,lat,lon)
}



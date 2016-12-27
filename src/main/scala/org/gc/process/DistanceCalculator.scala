package org.gc.process

import com.redis.RedisClientPool
import com.typesafe.scalalogging.Logger
import org.gc.data.Coordinate

/**
  * Process task
  *
  * Created by joao on 26/12/16.
  */
object DistanceCalculator {

  val log = Logger("DistanceCalculator")

  def processGeoDistance(inputCoords: Iterator[Coordinate], radius: Double)
                        (calc: (Double, Double, Double) => String) = {
    // Process in parallel
    inputCoords.toStream.par map (coord => {
      coord.id + "," + calc(coord.lat, coord.lon, radius) + "\n"
    })
 }
}
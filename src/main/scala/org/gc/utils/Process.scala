package org.gc.utils

import java.io.OutputStream

import com.redis.RedisClientPool
import com.typesafe.scalalogging.Logger

/**
  * Process task
  *
  * Created by joao on 26/12/16.
  */
object Process {

  val log = Logger("Distance")

  def calc(lat1: Double, lng1: Double, lat2: Double, lng2: Double): Double = {
    val earthRadius: Double = 6371000
    //meters
    val dLat = Math.toRadians(lat2 - lat1)
    val dLng = Math.toRadians(lng2 - lng1)
    val a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
      Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
        Math.sin(dLng / 2) * Math.sin(dLng / 2)
    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
    (earthRadius * c)
  }

  def processGeoDistance(inputLines: Iterator[String], outStream: OutputStream, connection: RedisClientPool, radius: Double) = {

    log.info ("Read all inputs")
    // Read all input line
    val inputs = inputLines map (line => {
      val Array(id, lat, lon) = line.split(",").map(_.trim)
      (id, lat.toDouble, lon.toDouble)
    })

    log.info ("Calculating distances")
    // Process in parallel
    val dists = inputs.toList.par map (input => {
      val id = input._1
      val lat = input._2
      val long = input._3

      RedisUtils.findDistance(lat, long, radius, connection) match {
        case Some(list) => {
          if (list.size == 1) {
            val res = list.toList
            id + "," + res(0).get.member.get + "\n"

          } else {
            id + ",\n"
          }
        }
        case None => id + ",\n"
      }
    })

    log.info ("Writing to output file")
    // For each distance result push to the output file
    dists.foreach(dist => {
      outStream.write(dist.getBytes)
    })
  }

}

package org.gc.utils

import com.redis.RedisClientPool
import com.typesafe.scalalogging.Logger

/**
  * Redis utils
  *
  * @author : Joao Costa (joaocarlosfilho@gmail.com) on 23/12/2016.
  *
  */
object RedisUtils {

  val log = Logger("RedisUtils")

  def connect(host: String, port: Integer) = new RedisClientPool(host, port)

  def pushLinesToRedisGeo(lines: Iterator[String], connection: RedisClientPool) = {
    connection.withClient {
      client => {
        lines.foreach(line => {
          val Array(id, lat, long) = line.split(",").map(_.trim)
          log.info("{}:{}:{}", id, lat, long)
          client.geoadd("APP", Seq((long.toFloat, lat.toFloat, id)))
        })
      }
    }
  }

  def findDistance(lat: Double, lon: Double, rad: Double, connection: RedisClientPool) = {
    connection.withClient {
      client => {
        client.georadius("APP", lon, lat, rad, "km", false, false, false, Some(1), None, None, None)
      }
    }
  }

}

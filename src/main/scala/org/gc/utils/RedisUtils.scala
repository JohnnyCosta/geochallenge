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

  def pushToRedisGeoFromLines(lines: Iterator[String], connection: RedisClientPool) = {
    connection.withClient {
      client => {
        lines.foreach(line => {
          val Array(id, lat, long) = line.split(",").map(_.trim)
          client.geoadd("APP", Seq((long.toFloat, lat.toFloat, id)))
        })
      }
    }
  }

  def redisCalculateDistance(lat: Double, lon: Double, rad: Double, connection: RedisClientPool) = {
    RedisUtils.findByRadius(lat, lon, rad, connection) match {
      case Some(list) => {
        if (list.size == 1) {
          val res = list.toList
          res(0).get.member.get
        } else {
          ""
        }
      }
      case None => ""
    }
  }

  private def findByRadius(lat: Double, lon: Double, rad: Double, connection: RedisClientPool) = {
    connection.withClient {
      client => {
        client.georadius("APP", lon, lat, rad, "km", false, false, false, Some(1), None, None, None)
      }
    }
  }

}

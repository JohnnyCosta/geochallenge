package org.gc.redis

import com.redis.RedisClientPool
import com.typesafe.scalalogging.Logger

import scala.io.Source

/**
  * Redis utils
  *
  * @author : Joao Costa (joaocarlosfilho@gmail.com) on 23/12/2016.
  *
  */
object RedisUtils {

  val log = Logger("RedisUtils")

  def redisClient(host: String, port: Integer) = new RedisClientPool(host, port)

  def redisAddGeoDataFromLines(input: Source, connection: RedisClientPool, skipHeader: Boolean = true) = {
    val lines = if (skipHeader==true) input.getLines().drop(1) else input.getLines()
    connection.withClient {
      client => {
        lines.foreach(line => {
          val Array(id, lat, long) = line.split(",").map(_.trim)
          client.geoadd("APP", Seq((long.toFloat, lat.toFloat, id)))
        })
      }
    }
  }

  def redisCalculate(lat: Double, lon: Double, rad: Double, connection: RedisClientPool) = {
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

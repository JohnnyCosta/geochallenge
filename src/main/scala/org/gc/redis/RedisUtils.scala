package org.gc.redis

import com.redis.RedisClientPool
import com.typesafe.scalalogging.Logger
import org.gc.data.Coordinate

import scala.io.Source

/**
  * Redis utils
  *
  * @author : Joao Costa (joaocarlosfilho@gmail.com) on 26/12/16.
  *
  */
object RedisUtils {

  val log = Logger("RedisUtils")

  def redisClient(host: String, port: Integer) = new RedisClientPool(host, port)

  def redisAddGeoCoords(key: String, coords: Iterator[Coordinate], connection: RedisClientPool, skipHeader: Boolean = true) = {
    connection.withClient {
      client => {
        coords.foreach(coord => {
          client.geoadd(key, Seq((coord.lon, coord.lat, coord.id)))
        })
      }
    }
  }

  def redisCoordByKey(key: String,name:String, connection: RedisClientPool) = {
    connection.withClient {
      client => {
        client.geopos(key,name)
      }
    }
  }

  def redisCalculate(key: String, lat: Double, lon: Double, rad: Double, connection: RedisClientPool) = {
    RedisUtils.findByRadius(key, lat, lon, rad, connection) match {
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

  private def findByRadius(key: String, lat: Double, lon: Double, rad: Double, connection: RedisClientPool) = {
    connection.withClient {
      client => {
        client.georadius(key, lon, lat, rad, "km", false, false, false, Some(1), None, None, None)
      }
    }
  }

}

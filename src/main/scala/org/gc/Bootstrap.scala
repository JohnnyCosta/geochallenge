package org.gc

import com.redis._
import com.typesafe.scalalogging.Logger
import jdk.nashorn.internal.runtime.linker.Bootstrap
import org.gc.utils.FileUtils.readGZIPFile
import org.gc.utils.{FileUtils, RedisUtils}

import scala.io.Source.fromInputStream

/**
  * Bootstrap for application
  *
  * @author : Joao Costa (joaocarlosfilho@gmail.com) on 23/12/2016.
  *
  */
object Bootstrap extends App {

  val logger = Logger(classOf[Bootstrap])

  val config = FileUtils.readYML("app.yml")

  val lines = fromInputStream(readGZIPFile(config.data.folder + config.data.file)).getLines().drop(1)

  val redis = RedisUtils.connect(config.redis.host, config.redis.port)

  pushLines(lines, redis)

  def geoAdd(body: RedisClient => ) = {
    val client = pool.borrowObject
    try {
      body(client)
    } finally {
      pool.returnObject(client)
    }
  }

  def pushLines(lines: Iterator[String], connection: RedisClientPool) = {
    val clients =
      connection.withClient {
        client => {
          lines.foreach(line => {
            val Array(id, lat, long) = line.split(",").map(_.trim)
            logger.info("{}:{}:{}", id, lat, long)
            client.geoadd("APP", Seq((long.toFloat, lat.toFloat, id)))
          })
        }
      }
  }
}

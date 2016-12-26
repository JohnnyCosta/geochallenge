package org.gc

import com.typesafe.scalalogging.Logger
import jdk.nashorn.internal.runtime.linker.Bootstrap
import org.gc.utils.Process.processGeoDistance
import org.gc.utils.FileUtils.readFromGZIPFile
import org.gc.utils.RedisUtils.pushLinesToRedisGeo
import org.gc.utils.{FileUtils, RedisUtils}

import scala.io.Source.fromInputStream

/**
  * Bootstrap application
  *
  * @author : Joao Costa (joaocarlosfilho@gmail.com) on 23/12/2016.
  *
  */
object Bootstrap extends App {

  val log = Logger(classOf[Bootstrap])

  val config = FileUtils.readYML("app.yml")

  val data = fromInputStream(readFromGZIPFile(config.data.folder + config.data.file)).getLines().drop(1)

  val input = fromInputStream(readFromGZIPFile(config.input.folder + config.input.file)).getLines().drop(1)

  val output = FileUtils.writeToGZIPFile(config.output.folder + config.output.file)

  val redis = RedisUtils.connect(config.redis.host, config.redis.port)

  pushLinesToRedisGeo(data, redis)

  processGeoDistance(input, output, redis, config.redis.distance.toDouble)

  output.close()
  redis.close

}

package org.gc

import com.typesafe.scalalogging.Logger
import org.gc.io.FileUtils._
import org.gc.process.DistanceCalculator.processGeoDistance
import org.gc.redis.RedisUtils._

/**
  * Bootstrap application
  *
  * @author : Joao Costa (joaocarlosfilho@gmail.com) on 23/12/2016.
  *
  */
object AppBootstrap extends App {

  val log = Logger("AppBootstrap")

  val config = readYML("app.yml")

  val redis = redisClient(config.redis.host, config.redis.port)

  val dataPath = config.data.folder + config.data.file
  log.info("Adding data from '{}' to redis", dataPath)
  val dataCoords = generateCoordinatesFromLines(readGZIPFile(dataPath))
  redisAddGeoCoords(config.redis.key, dataCoords, redis)

  val inputPath = config.input.folder + config.input.file
  log.info("Read all inputs from '{}'", inputPath)
  val inCoords = generateCoordinatesFromLines(readGZIPFile(inputPath))

  log.info("Calculating distances")
  val radius = config.redis.geoCalcRadius.toDouble

  def redisCal = (lat: Double, lon: Double, rad: Double) => {
    redisCalculate(config.redis.key, lat, lon, rad, redis)
  }

  val outLines = processGeoDistance(inCoords, radius)(redisCal)

  val outputPath = config.output.folder + config.output.file
  val output = writeToGZIPFile(outputPath)
  log.info("Writing to output file '{}'", outputPath)
  writeLines(outLines, output)

  output.close()
  redis.close

}

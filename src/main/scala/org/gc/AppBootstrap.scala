package org.gc

import com.typesafe.scalalogging.Logger
import org.gc.process.DistanceCalculator.processGeoDistance
import org.gc.io.FileUtils._
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
  redisAddGeoDataFromLines(readGZIPFile(dataPath), redis)

  val inputPath = config.input.folder + config.input.file
  log.info("Read all inputs from '{}'", inputPath)
  val coords = generateCoordinatesFromLines(readGZIPFile(inputPath))

  log.info("Calculating distances")
  val radius = config.redis.geoCalcRadius.toDouble
  def redisCal = (lat: Double, lon: Double, rad: Double) => {
    redisCalculate(lat, lon, rad, redis)
  }
  val outLines = processGeoDistance(coords, radius)(redisCal)

  val outputPath = config.output.folder + config.output.file
  val output = writeToGZIPFile(outputPath)
  log.info("Writing to output file '{}'", outputPath)
  writeLines(outLines, output)

  output.close()
  redis.close

}

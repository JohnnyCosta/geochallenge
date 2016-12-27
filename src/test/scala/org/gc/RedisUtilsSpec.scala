package org.gc

import org.gc.data.Coordinate
import org.gc.process.DistanceCalculator.processGeoDistance
import org.gc.redis.RedisUtils._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

/**
  * Test what is possible from redis integration
  *
  * Created by joao on 27/12/16.
  */
@RunWith(classOf[JUnitRunner])
class RedisUtilsSpec extends FlatSpec with Matchers {

  "RedisUtils" should "add geographical coodinates" in {
    val client = redisClient("localhost", 6379)
    val inputCoords = Array(Coordinate("A", 50.06638888888889, 5.714722222222222), Coordinate("B", 58.64388888888889, 3.0700000000000003)).toList.iterator
    redisAddGeoCoords("TEST", inputCoords, client)

    val coor1 = redisCoordByKey("TEST", "A", client)
    coor1 should not be (null)
    coor1.get.toList.size should be(1)
    coor1.get.toList(0).get.toList.size should be(2)
    assert(coor1.get.toList(0).get.toList(0).get.toDouble === 5.714722222222222 +- 0.00001)
    assert(coor1.get.toList(0).get.toList(1).get.toDouble === 50.06638888888889 +- 0.00001)

    val coor2 = redisCoordByKey("TEST", "B", client)
    coor2 should not be (null)
    coor2.get.toList.size should be(1)
    coor2.get.toList(0).get.toList.size should be(2)
    assert(coor2.get.toList(0).get.toList(0).get.toDouble === 3.0700000000000003 +- 0.00001)
    assert(coor2.get.toList(0).get.toList(1).get.toDouble === 58.64388888888889 +- 0.00001)
  }

  "RedisUtils" should "calculate closest distance" in {
    val client = redisClient("localhost", 6379)
    val inputCoords = Array(Coordinate("A", 50.06638888888889, 5.714722222222222), Coordinate("B", 58.64388888888889, 3.0700000000000003)).toList.iterator
    redisAddGeoCoords("TEST", inputCoords, client)

    def redisCal = (lat: Double, lon: Double, rad: Double) => {
      redisCalculate("TEST", lat, lon, rad, client)
    }

    val inputCoordsCalc = Array(Coordinate("C",50.1, 5.8)).toList.iterator
    val outLines = processGeoDistance(inputCoordsCalc, 10.0)(redisCal)
    outLines should not be (null)
    val output = Array("C,A\n")
    outLines should equal(output)

    val inputCoordsCalc2 = Array(Coordinate("D",58.7, 3.1)).toList.iterator
    val outLines2 = processGeoDistance(inputCoordsCalc2, 10.0)(redisCal)
    outLines2 should not be (null)
    val output2 = Array("D,B\n")
    outLines2 should equal(output2)

    val inputCoordsCalc3 = Array(Coordinate("E",20, -2)).toList.iterator
    val outLines3 = processGeoDistance(inputCoordsCalc3, 10.0)(redisCal)
    outLines3 should not be (null)
    val output3 = Array("E,\n")
    outLines3 should equal(output3)
  }

}

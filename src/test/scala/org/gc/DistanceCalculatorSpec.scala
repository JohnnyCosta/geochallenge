package org.gc

import org.gc.data.Coordinate
import org.gc.process.DistanceCalculator.processGeoDistance
import org.gc.io.FileUtils.readFile
import org.gc.redis.RedisUtils.redisCalculate
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

/**
  * Test distance calculator
  *
  * Created by joao on 26/12/16.
  */
@RunWith(classOf[JUnitRunner])
class DistanceCalculatorSpec extends FlatSpec with Matchers {

  "DistanceCalculator" should "do a simple calculation" in {
    val inputCoords = Array(Coordinate("A",1,2),Coordinate("B",3,4),Coordinate("B",5,6)).toList.iterator

    def distCal = (lat: Double, lon: Double, rad: Double) => {
      if (lat==1 && lon==2){
        "A"
      } else if (lat==3 && lon==4){
        "B"
      } else {
        ""
      }
    }
    val outLines = processGeoDistance(inputCoords, 10)(distCal)
    outLines should not be (null)

    val output = Array("A,A\n","B,B\n","B,\n")
    outLines should equal(output)
  }
}

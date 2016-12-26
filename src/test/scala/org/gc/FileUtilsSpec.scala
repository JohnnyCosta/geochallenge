package org.gc

import java.io.InputStream

import org.gc.data.Coordinate
import org.junit.runner.RunWith

import collection.mutable.Stack
import org.scalatest._
import org.scalatest.junit.JUnitRunner
import org.gc.utils.FileUtils._

import scala.io.Source.fromInputStream

/**
  * Test file utils
  *
  * Created by joao on 26/12/16.
  */
@RunWith(classOf[JUnitRunner])
class FileUtilsSpec extends FlatSpec with Matchers {

  "FileUtils" should "read a file from resources" in {
    val stream = readFile("input/input.csv")
    stream should not be (null)
    stream.getLines().size should be (10)
  }

  it should "read a GZIP file from resources" in {
    val stream = readGZIPFile("input/input.csv.gz")
    stream should not be (null)
    stream.getLines().size should be (10)
  }

  it should "convert lines to coordinates" in {
    val iterCoords = generateCoordinatesFromLines(readGZIPFile("input/input.csv.gz"),true)
    iterCoords should not be null
    val coords = iterCoords.toList
    coords.size should be (9)
    val first = coords(0)
    first.id should be ("DDEFEBEA-98ED-49EB-A4E7-9D7BFDB7AA0B")
    first.lat should be (-37.83330154418945)
    first.lon should be (145.0500030517578)
  }

  it should "throw NumberFormatException if file contains a header" in {
    a [NumberFormatException] should be thrownBy {
      val iterCoords = generateCoordinatesFromLines(readGZIPFile("input/input.csv.gz"),false)
      val coords = iterCoords.toList
    }
  }


  it should "read YML file configuration" in {
    val config = readYML("app-test.yml")
    config should not be (null)
    config.redis.host should be ("localhost")
    config.redis.port should be (6379)
    config.redis.geoCalcRadius should be ("10.0")
    config.output.folder should be ("/tmp/")
    config.output.file should be ("output.csv.gz")
    config.input.folder should be ("input/")
    config.input.file should be ("input.csv.gz")
    config.data.folder should be ("data/")
    config.data.file should be ("data.csv.gz")
  }
}

package org.gc

import java.io.{File, FileInputStream}
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.zip.GZIPInputStream

import org.gc.data.Coordinate
import org.gc.io.FileUtils._
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

import scala.io.Source
import scala.io.Source.fromInputStream
import scala.util.Random

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
    stream.getLines().size should be(10)
  }

  it should "read a GZIP file from resources" in {
    val stream = readGZIPFile("input/input.csv.gz")
    stream should not be (null)
    stream.getLines().size should be(10)
  }

  it should "convert lines to coordinates" in {
    val iterCoords = generateCoordinatesFromLines(readGZIPFile("input/input.csv.gz"), true)
    iterCoords should not be null
    val coords = iterCoords.toList
    coords.size should be(9)
    val first = coords(0)
    first.id should be("DDEFEBEA-98ED-49EB-A4E7-9D7BFDB7AA0B")
    first.lat should be(-37.83330154418945)
    first.lon should be(145.0500030517578)
  }

  it should "throw NumberFormatException if file contains a header" in {
    a[NumberFormatException] should be thrownBy {
      val iterCoords = generateCoordinatesFromLines(readGZIPFile("input/input.csv.gz"), false)
      val coords = iterCoords.toList
    }
  }

  def generateUniqueFileName() {
    val now = Calendar.getInstance().getTime();
    val format = new SimpleDateFormat("MMddyyyyHHmmssS");
    format.format(now)
  }

  "FileUtils" should "write to a file" in {
    val tempFile = File.createTempFile("output", ".csv");
    val output = writeTo(tempFile.getAbsolutePath)
    val randtext = Random.alphanumeric.take(20).mkString
    output.write(randtext.getBytes)
    output.close()

    val stream = Source.fromFile(tempFile.getAbsolutePath)
    stream should not be (null)
    val lines = stream.getLines().toList
    lines.size should be(1)
    lines(0) should be (randtext)
  }

  "FileUtils" should "write to GZIP file" in {
    val tempFile = File.createTempFile("output", ".csv.gz");
    val output = writeToGZIPFile(tempFile.getAbsolutePath)
    val randtext = Random.alphanumeric.take(20).mkString
    output.write(randtext.getBytes)
    output.close()

    val stream = fromInputStream(new GZIPInputStream(new FileInputStream(tempFile.getAbsolutePath)))
    stream should not be (null)
    val lines = stream.getLines().toList
    lines.size should be(1)
    lines(0) should be (randtext)
  }

  "FileUtils" should "write lines to a file" in {
    val tempFile = File.createTempFile("output", ".csv.gz");
    val output = writeToGZIPFile(tempFile.getAbsolutePath)
    val lines = Array("A\n","B\n").toList.par
    writeLines(lines,output)
    output.close()

    val stream = fromInputStream(new GZIPInputStream(new FileInputStream(tempFile.getAbsolutePath)))
    stream should not be (null)
    val outlines = stream.getLines().toList
    outlines.size should be(2)
  }

  it should "read YML file configuration" in {
    val config = readYML("app-test.yml")
    config should not be (null)
    config.redis.host should be("localhost")
    config.redis.port should be(6379)
    config.redis.geoCalcRadius should be("10.0")
    config.output.folder should be("/tmp/")
    config.output.file should be("output.csv.gz")
    config.input.folder should be("input/")
    config.input.file should be("input.csv.gz")
    config.data.folder should be("data/")
    config.data.file should be("data.csv.gz")
  }

  it should "return null for invalid YML file" in {
    val config = readYML("app-test-invalid.yml")
    config should be (null)
  }
}

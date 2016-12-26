package org.gc.utils

import java.io.{FileOutputStream, InputStream, OutputStream}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import org.gc.data.{Config, Coordinate}
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor

import scala.collection.parallel.immutable.ParSeq
import scala.io.Source
import scala.io.Source.fromInputStream

/**
  * Group all file operations
  *
  * @author : Joao Costa (joaocarlosfilho@gmail.com) on 23/12/2016.
  *
  */
object FileUtils {

  private def readFromResource(name: String): InputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(name)

  def readFile(name: String): Source = fromInputStream(readFromResource(name))

  def readGZIPFile(name: String): Source = fromInputStream(new GZIPInputStream(readFromResource(name)))

  def generateCoordinatesFromLines(input: Source, skipHeader: Boolean) = {
    val lines = if (skipHeader==true) input.getLines().drop(1) else input.getLines()
    lines map (line => {
      val Array(id, lat, lon) = line.split(",").map(_.trim)
      Coordinate(id, lat.toDouble, lon.toDouble)
    })
  }

  def writeTo(name: String): OutputStream = new FileOutputStream(name)

  def writeToGZIPFile(name: String): OutputStream = new GZIPOutputStream(writeTo(name))

  def writeLines(lines: ParSeq[String], outStream: OutputStream) {
    lines.foreach(line => {
      outStream.write(line.getBytes)
    })
  }

  def readYML(name: String): Config = new Yaml(new Constructor(classOf[Config])).load(readFromResource(name)).asInstanceOf[Config]

}

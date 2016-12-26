package org.gc.utils

import java.io.{FileOutputStream, InputStream, OutputStream}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import org.gc.data.Config
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor

/**
  * Group all file operations
  *
  * @author : Joao Costa (joaocarlosfilho@gmail.com) on 23/12/2016.
  *
  */
object FileUtils {

  def readFromResource(name: String): InputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(name)

  def readFromGZIPFile(name: String): InputStream = new GZIPInputStream(readFromResource(name))

  def writeTo(name: String): OutputStream = new FileOutputStream(name)

  def writeToGZIPFile(name: String): OutputStream = new GZIPOutputStream(writeTo(name))

  def readYML(name: String): Config = new Yaml(new Constructor(classOf[Config])).load(readFromResource(name)).asInstanceOf[Config]

}

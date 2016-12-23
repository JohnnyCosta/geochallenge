package org.gc.data

import scala.beans.BeanProperty

/**
  * YML configuration data object
  *
  * @author : Joao Costa (joaocarlosfilho@gmail.com) on 23/12/2016.
  *
  */


class FileFolder() {
  @BeanProperty var folder: String = null
  @BeanProperty var file: String = null
}

class Config() {
  @BeanProperty var output: FileFolder = null
  @BeanProperty var input: FileFolder = null
  @BeanProperty var data: FileFolder = null
}
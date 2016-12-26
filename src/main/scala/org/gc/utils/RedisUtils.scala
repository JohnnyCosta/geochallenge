package org.gc.utils

import java.io.InputStream

import com.redis.RedisClientPool

/**
  * Redis utils
  *
  * @author : Joao Costa (joaocarlosfilho@gmail.com) on 23/12/2016.
  *
  */
object RedisUtils {

  def connect(host: String, port: Integer) = new RedisClientPool(host, port)

}

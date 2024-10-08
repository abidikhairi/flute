package com.majesteye.rnd.flute
package utils

object Constants {

  val SPARK_MASTER_URL: String = "local[*]"
  val DEFAULT_READ_OPTIONS: Map[String, String] = Map("header" -> "true", "delimiter" -> "\t")
  val DEFAULT_WRITE_OPTIONS: Map[String, String] = Map("header" -> "true", "delimiter" -> "\t")
  val DEFAULT_WRITE_FORMAT: String = "csv"
}

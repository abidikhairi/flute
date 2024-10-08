package com.majesteye.rnd.flute
package utils

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SparkUtils {

  def sparkSession(appName: String): SparkSession = {
    val spark = SparkSession.builder()
      .master(Constants.SPARK_MASTER_URL)
      .appName(appName)
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    spark
  }

  def saveDataFrame(df: DataFrame, savePath: String, numPartitions: Int = 10): Unit = {
    println(s"Saving dataframe to: ${savePath}")
    df.coalesce(numPartitions)
      .write
      .format(Constants.DEFAULT_WRITE_FORMAT)
      .options(Constants.DEFAULT_WRITE_OPTIONS)
      .mode(SaveMode.Overwrite)
      .save(savePath)
  }
}

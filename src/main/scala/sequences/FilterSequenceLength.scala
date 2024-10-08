package com.majesteye.rnd.flute
package sequences

import utils.{Constants, SparkUtils}

import org.apache.spark.sql.functions.{col, expr}

object FilterSequenceLength {

  private val MAX_SEQ_LEN: Int = 1024

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Usage: FilterSequenceLength <path/to/input.tsv> <path/to/output>")
      System.exit(1)
    }

    val inputFile = args(0)
    val outputFile = args(1)

    val spark = SparkUtils.sparkSession("FilterSequenceByLength")

    println(s"Loading data from: ${inputFile}")
    val sequenceDF = spark.read
      .format("csv")
      .options(Constants.DEFAULT_READ_OPTIONS)
      .load(inputFile)

    println(s"Num Rows before filter: ${sequenceDF.count()}")
    println(s"Filtering sequences with max length: ${MAX_SEQ_LEN}")
    val filteredSequenceDF = sequenceDF.where(expr(s"char_length(Sequence) <= ${MAX_SEQ_LEN}"))
    println(s"Num Rows after filter: ${filteredSequenceDF.count()}")

    SparkUtils.saveDataFrame(filteredSequenceDF, outputFile, 1)

    spark.stop()
  }
}

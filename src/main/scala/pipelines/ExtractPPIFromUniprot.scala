package com.majesteye.rnd.flute
package pipelines

import utils.{Constants, SparkUtils}

import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.functions.{col, element_at, explode, regexp_extract, regexp_replace, split, trim, when}

object ExtractPPIFromUniprot {

  private val interactsWithColumnName = "Interacts with"
  private val interactsWithNewColumnName = "InteractsWith"

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Usage: ExtractPPIFromUniprot <path/to/input.tsv> <path/to/output>")
      System.exit(1)
    }

    val inputFile = args(0)
    val outputFile = args(1)

    val spark = SparkUtils.sparkSession("ExtractPPI")

    val entryDF = spark.read
      .format("csv")
      .options(Constants.DEFAULT_READ_OPTIONS)
      .load(inputFile)
      .na.drop()

    println(s"Loaded dataframe number of rows: ${entryDF.count()}")
    val entryRenamedDF = entryDF.withColumnRenamed(interactsWithColumnName, interactsWithNewColumnName)

    val ppiDF = entryRenamedDF
      .withColumn(interactsWithNewColumnName, functions.split(col(interactsWithNewColumnName), ";"))
      .withColumn(interactsWithNewColumnName, explode(col(interactsWithNewColumnName)))
      .na.drop()

    val normalizedPpiDF = normalizeInteractsWithColumn(ppiDF)
    val ppiWithIsoforms = this.extractIsoforms(normalizedPpiDF)

    println(s"Saving dataframe with number of rows: ${ppiWithIsoforms.count()}")

    SparkUtils.saveDataFrame(ppiWithIsoforms, outputFile, 1)

    spark.stop()
  }

  private def normalizeInteractsWithColumn(ppiDF: DataFrame): DataFrame = {
    ppiDF.withColumn("Entry2", when(col(interactsWithNewColumnName).contains("PRO_").or(col(interactsWithNewColumnName).contains("[")),
        regexp_extract(col(interactsWithNewColumnName), """\[[A-Z0-9]+\]""", 0)
      ).otherwise(col(interactsWithNewColumnName))
    )
      .withColumn("Entry2", regexp_replace(col("Entry2"), """\[""", ""))
      .withColumn("Entry2", regexp_replace(col("Entry2"), """\]""", ""))
      .withColumn("Entry2", trim(col("Entry2")))
      .withColumn(interactsWithNewColumnName, col("Entry2"))
      .drop("Entry2")
  }

  private def extractIsoforms(df: DataFrame): DataFrame = {
    df.withColumn(interactsWithNewColumnName, trim(col(interactsWithNewColumnName)))
      .withColumn("Isoform", when(col(interactsWithNewColumnName).contains("-"),
          element_at(split(col(interactsWithNewColumnName), "-"), 2)
        ).otherwise(null)
      )
      .withColumn(interactsWithNewColumnName, when(col(interactsWithNewColumnName).contains("-"),
          element_at(split(col(interactsWithNewColumnName), "-"), 1)
        ).otherwise(col(interactsWithNewColumnName))
      )
  }
}

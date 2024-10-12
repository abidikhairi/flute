package com.majesteye.rnd.flute
package pipelines

import utils.{Constants, SparkUtils}
import org.apache.spark.sql.functions._

object ExtractProteinNames {

  private val columnsNameMap: Map[String, String] = Map(
    "Entry Name" -> "EntryName",
    "Protein names" -> "ProteinNames",
    "EC number" -> "EcNumber",
    "Organism (ID)6" -> "OrganismID"
  )

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println(s"Usage: ${ExtractProteinNames.getClass} <path/to/input.tsv> <path/to/output>")
      System.exit(1)
    }

    val inputFile = args(0)
    val outputFile = args(1)

    val spark = SparkUtils.sparkSession("ExtractProteinNames")

    var metadataDF = spark.read
      .format("csv")
      .options(Constants.DEFAULT_READ_OPTIONS)
      .load(inputFile)

    for ((oldName, newName) <- columnsNameMap) {
      metadataDF = metadataDF.withColumnRenamed(oldName, newName)
    }
    val pattern = """\((.*?)\)""".r

    val extractPrimaryName = udf((value: String) => value.replace(pattern.findAllIn(value).mkString(" "), ""))

    metadataDF = metadataDF.select("Entry", "EntryName", "ProteinNames", "EcNumber", "OrganismID")

    val proteinNamesDF = metadataDF
      .withColumn("PrimaryName", extractPrimaryName(col("ProteinNames")))
      .withColumn("PrimaryName", trim(col("PrimaryName")))
      .drop("ProteinNames")


        SparkUtils.saveDataFrame(
          proteinNamesDF.select("Entry", "EntryName", "PrimaryName", "OrganismID"),
          s"${outputFile}/entry_primary_name"
        )

    val proteinWithEC = metadataDF.withColumn("EcNumber", split(col("EcNumber"), ";"))
      .withColumn("EcNumber", explode(col("EcNumber")))
      .filter(col("EcNumber").isNotNull)
      .withColumn("RelType", lit("CATALYZES"))
      .select("Entry", "RelType", "EcNumber")

    SparkUtils.saveDataFrame(
      proteinWithEC,
    s"${outputFile}/protein_ec_number_link"
    )

    spark.stop()
  }
}

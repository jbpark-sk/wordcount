
package jbpark.wordcount

import com.typesafe.scalalogging.StrictLogging


object Main extends Spark with StrictLogging {

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      logger.error("Usage: wordcount <inputPath> <outputPath>")
      System.exit(1)
    }

    val inputPath: String = args(0)
    val outputPath: String = args(1)
    logger.info(s"inputPath=${inputPath} outputPath=${outputPath}")

    WordCounter.countWord(spark.read.textFile(inputPath))
      .coalesce(1)
      .write.mode("overwrite")
      .option("header", true)
      .csv(outputPath)
  }

}


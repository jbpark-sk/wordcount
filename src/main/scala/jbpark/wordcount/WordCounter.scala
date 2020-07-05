
package jbpark.wordcount

import org.apache.spark.sql.{Dataset, Row}


object WordCounter extends Spark {

  import spark.implicits._


  def countWord(ds: Dataset[String]): Dataset[Row] = {
    ds.flatMap(_.split(raw"\W+").filterNot(_.isEmpty).map(_.toLowerCase))
      .toDF("word")
      .createOrReplaceTempView("v_word")

    spark.sql("""
      |SELECT word, COUNT(*) AS `count`
      |FROM v_word
      |GROUP BY word
      |ORDER BY `count` DESC, word ASC
      |""".stripMargin.trim)
  }

}



package jbpark.wordcount

import org.apache.spark.sql.SparkSession


trait Spark {

  @transient
  protected lazy val spark: SparkSession = {
    SparkSession.builder
      .appName("wordcount")
      .getOrCreate
  }

}


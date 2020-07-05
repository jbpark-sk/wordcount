
package jbpark.wordcount

import org.apache.spark.sql.SparkSession


trait SparkTest {

  @transient
  protected val spark: SparkSession = {
    SparkSession.builder
      .master("local[*]")
      .config("spark.testing", true)
      .getOrCreate
  }

}


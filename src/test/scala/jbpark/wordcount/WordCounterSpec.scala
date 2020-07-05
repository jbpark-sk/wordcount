
package jbpark.wordcount

import org.apache.spark.sql.Row
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers


class WordCounterSpec extends AnyFreeSpec with Matchers with SparkTest {

  import spark.implicits._


  def callWordCounter(input: String) = {
    val ds = WordCounter.countWord(input.split(raw"\n").toSeq.toDS)

    ds.show(5)

    ds.collect.toSeq
  }


  "A single line" in {
    val input = "If you think you can do it, you can do it."

    val expected = Seq(
      Row("you", 3),
      Row("can", 2),
      Row("do", 2),
      Row("it", 2),
      Row("if", 1),
      Row("think", 1),
    )

    val actual = callWordCounter(input)

    actual shouldBe expected
  }

  "Multiple lines" in {
    val input = """
      |The woods are lovely, dark and deep,
      |But I have promises to keep,
      |  And miles to go before I sleep,
      |  And miles to go before I sleep.
      |""".stripMargin.trim

    val expected = Seq(
      Row("and", 3),
      Row("i", 3),
      Row("to", 3),
      Row("before", 2),
      Row("go", 2),
      Row("miles", 2),
      Row("sleep", 2),
      Row("are", 1),
      Row("but", 1),
      Row("dark", 1),
      Row("deep", 1),
      Row("have", 1),
      Row("keep", 1),
      Row("lovely", 1),
      Row("promises", 1),
      Row("the", 1),
      Row("woods", 1),
    )

    val actual = callWordCounter(input)

    actual shouldBe expected
  }

}


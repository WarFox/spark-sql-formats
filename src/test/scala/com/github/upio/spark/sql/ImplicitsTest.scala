package com.github.upio.spark.sql

import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

import org.apache.spark.sql.Row

/**
  * @author upio
  */
class ImplicitsTest extends FlatSpec with MockitoSugar with Matchers {

  import com.github.upio.spark.sql.Implicits._

  case class Test(a: String)
  val row = Row("a")
  val test = Test("a")
  class TestScope {
    implicit val testFormat = mock[RowFormat[Test]]
  }

  "RowWrapper#convertTo" should "proxy read on the RowFormat" in new TestScope {
    row.convertTo[Test]
    verify(testFormat).read(row)
  }

  "AnyWrapper#toRow" should "proxy write on the RowFormat" in new TestScope {
    test.toRow
    verify(testFormat).write(test)
  }
}

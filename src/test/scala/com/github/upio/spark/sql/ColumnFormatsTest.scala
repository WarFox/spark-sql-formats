package com.github.upio.spark.sql

import java.io.ObjectOutputStream
import java.sql.{Date, Timestamp}
import java.time.Instant

import org.apache.commons.io.output.NullOutputStream
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.IntegerType

import ColumnFormats._

/**
  * @author upio
  */
class ColumnFormatsTest extends FlatSpec with Matchers with MockitoSugar {
  val now = Instant.now()
  val date = new Date(20160101)
  val ts = new Timestamp(now.toEpochMilli)

  case class TestClass(a: String)
  implicit val testClassFormat = DefaultColumnFormat[TestClass](IntegerType)

  "booleanColumn" should "pass original boolean through" in {
    booleanColumn.write(true) shouldEqual true
  }

  it should "cast all Boolean types to boolean" in {
    booleanColumn.read(true) shouldEqual true
    booleanColumn.read(false) shouldEqual false
    booleanColumn.read(java.lang.Boolean.TRUE) shouldEqual true
    booleanColumn.read(java.lang.Boolean.FALSE) shouldEqual false
    booleanColumn.read(Boolean.box(true)) shouldEqual true
    booleanColumn.read(Boolean.box(false)) shouldEqual false
  }

  it should "throw ColumnDeserializationException if Any cannot be casted" in {
    intercept[ColumnDeserializationException](booleanColumn.read(""))
  }

  "byteColumn" should "pass original byte through" in {
    byteColumn.write(1: Byte) shouldEqual (1: Byte)
    byteColumn.write(Byte.box(1: Byte)) shouldEqual (1: Byte)
  }

  it should "cast all Byte types to Byte" in {
    byteColumn.read(1: Byte) shouldEqual (1: Byte)
    byteColumn.read(Byte.box(1: Byte)) shouldEqual (1: Byte)
  }

  it should "throw ColumnDeserializationException if Any cannot be casted" in {
    intercept[ColumnDeserializationException](byteColumn.read(""))
  }

  "shortColumn" should "pass original boolean through" in {
    shortColumn.write(1: Short) shouldEqual (1: Short)
  }

  it should "cast all Short types to Short" in {
    shortColumn.read(1: Short) shouldEqual (1: Short)
    shortColumn.read(Short.box(1)) shouldEqual (1: Short)
  }

  it should "throw ColumnDeserializationException if Any cannot be casted" in {
    intercept[ColumnDeserializationException](shortColumn.read(""))
  }

  "intColumn" should "pass original int through" in {
    intColumn.write(1: Int) shouldEqual (1: Int)
  }

  it should "cast all Int types to Int" in {
    intColumn.read(1: Int) shouldEqual (1: Int)
    intColumn.read(Int.box(1)) shouldEqual (1: Int)
  }

  it should "throw ColumnDeserializationException if Any cannot be casted" in {
    intercept[ColumnDeserializationException](intColumn.read(""))
  }

  "longColumn" should "pass original long through" in {
    longColumn.write(1: Long) shouldEqual (1: Long)
  }

  it should "cast all Long types to Long" in {
    longColumn.read(1: Long) shouldEqual (1: Long)
    longColumn.read(Long.box(1)) shouldEqual (1: Long)
  }

  it should "throw ColumnDeserializationException if Any cannot be casted" in {
    intercept[ColumnDeserializationException](longColumn.read(""))
  }
  
  "floatColumn" should "pass original float through" in {
    floatColumn.write(1: Float) shouldEqual (1: Float)
  }

  it should "cast all Float types to Float" in {
    floatColumn.read(1: Float) shouldEqual (1: Float)
    floatColumn.read(Float.box(1)) shouldEqual (1: Float)
  }

  it should "throw ColumnDeserializationException if Any cannot be casted" in {
    intercept[ColumnDeserializationException](floatColumn.read(""))
  }
  
  "doubleColumn" should "pass original double through" in {
    doubleColumn.write(1: Double) shouldEqual (1: Double)
  }

  it should "cast all Double types to Double" in {
    doubleColumn.read(1: Double) shouldEqual (1: Double)
    doubleColumn.read(Double.box(1)) shouldEqual (1: Double)
  }

  it should "throw ColumnDeserializationException if Any cannot be casted" in {
    intercept[ColumnDeserializationException](doubleColumn.read(""))
  }
  
  "stringColumn" should "pass original String through" in {
    stringColumn.write("string") shouldEqual "string"
  }

  it should "cast Any to String" in {
    stringColumn.read("string") shouldEqual "string"
  }

  it should "throw ColumnDeserializationException if Any cannot be casted" in {
    intercept[ColumnDeserializationException](stringColumn.read(1))
  }
  
  "byteSeqColumn" should "pass original byte array through" in {
    byteSeqColumn.write(Array(1: Byte)) shouldEqual Array(1: Byte)
  }

  it should "cast Any to byte array" in {
    byteSeqColumn.read(Array(1: Byte)) shouldEqual Array(1: Byte)
  }

  it should "throw ColumnDeserializationException if Any cannot be casted" in {
    intercept[ColumnDeserializationException](byteSeqColumn.read(1))
  }

  "timestampColumn" should "pass original Timestamp through" in {
    timestampColumn.write(ts) shouldEqual ts
  }

  it should "cast Any to Timestamp" in {
    timestampColumn.read(ts) shouldEqual ts
  }

  it should "throw ColumnDeserializationException if Any cannot be casted" in {
    intercept[ColumnDeserializationException](timestampColumn.read(1))
  }

  "dateColumn" should "pass original Date through" in {
    dateColumn.write(date) shouldEqual date
  }

  it should "cast Any to Date" in {
    dateColumn.read(date) shouldEqual date
  }

  it should "throw ColumnDeserializationException if Any cannot be casted" in {
    intercept[ColumnDeserializationException](dateColumn.read(1))
  }

  "arrayColumn" should "map Array[TestClass] to Array[Any]" in {
    arrayColumn[TestClass].write(Array(TestClass("a"))) shouldEqual Array(TestClass("a"))
  }

  it should "deserialize Array[Any] to Array[TestClass]" in {
    arrayColumn[TestClass].read(Array(TestClass("a"))) shouldEqual Array(TestClass("a"))
  }

  it should "throw ColumnDeserializationException if Any cannot be casted" in {
    intercept[ColumnDeserializationException](arrayColumn[TestClass].read(1))
  }

  "seqColumn" should "map Seq[TestClass] to serialized Seq[Any]" in {
    seqColumn[TestClass].write(Seq(TestClass("a"))) shouldEqual Seq(TestClass("a"))
  }

  it should "deserialize Seq[Any] to Seq[TestClass]" in {
    seqColumn[TestClass].read(Seq(TestClass("a"))) shouldEqual Seq(TestClass("a"))
  }

  it should "throw ColumnDeserializationException if Any cannot be casted" in {
    intercept[ColumnDeserializationException](seqColumn[TestClass].read(1))
  }

  val map = Map(TestClass("key") -> TestClass("value"))
  "mapColumn" should "map Map[TestClass, TestClass] to Map[Any, Any]" in {
    mapColumn[TestClass, TestClass].write(map) shouldEqual map
  }

  it should "deserialize Map[TestClass, TestClass] to Map[Any, Any]" in {
    mapColumn[TestClass, TestClass].read(map) shouldEqual map
  }

  it should "throw ColumnDeserializationException if Any cannot be casted" in {
    intercept[ColumnDeserializationException](mapColumn[TestClass, TestClass].read(1))
  }

  implicit val mockRowFormat: RowFormat[TestClass] = mock[RowFormat[TestClass]]
  "structColumn" should "generate a RowFormat for a generic Product" in {
    when(mockRowFormat.write(TestClass("a"))) thenReturn Row("a")
    structColumn[TestClass].write(TestClass("a")) shouldEqual Row("a")
  }

  it should "deserialize Any to TestClass" in {
    when(mockRowFormat.read(Row("a"))) thenReturn TestClass("a")
    structColumn[TestClass].read(Row("a")) shouldEqual TestClass("a")
  }

  it should "throw ColumnDeserializationException if Any is not a Row" in {
    intercept[ColumnDeserializationException](structColumn[TestClass].read("a"))
  }

  it should "throw ColumnDeserializationException if Row cannot be parsed" in {
    when(mockRowFormat.read(Row("a"))) thenThrow ColumnDeserializationException("a")
    intercept[ColumnDeserializationException](structColumn[TestClass].read(Row("a"))) shouldEqual ColumnDeserializationException("a")
  }

  val instantFormat = CustomColumnFormat[Instant, Timestamp](
    instant => new Timestamp(instant.toEpochMilli),
    timestamp => timestamp.toInstant
  )
  "CustomTypeConverter" should "generate a ColumnFormat capable of writing a custom type" in {
     instantFormat.write(now) shouldEqual ts
  }

  it should "generate a ColumnFormat capable of reading a custom type" in {
    instantFormat.read(ts) shouldEqual now
  }

  "StandardFormats" should "be serializable" in {
    new ObjectOutputStream(new NullOutputStream).writeObject(new ColumnFormats {})
  }
}

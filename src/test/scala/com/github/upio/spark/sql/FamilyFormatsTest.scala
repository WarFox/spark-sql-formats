package com.github.upio.spark.sql

import java.io.ObjectOutputStream
import java.sql.Timestamp
import java.time.Instant

import org.apache.commons.io.output.NullOutputStream
import org.scalatest.{FlatSpec, Matchers}

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import ColumnFormats._
import FamilyFormats._

/**
  * @author upio
  */
class FamilyFormatsTest extends FlatSpec with Matchers {
  "FamilyFormats" should "generate a RowFormat for a case class" in {
    val format = rowFormat[FlatClass]
    format.write(FlatClass("a")) shouldEqual Row("a")
    format.read(Row("a")) shouldEqual FlatClass("a")
    format.schema shouldEqual StructType(Seq(StructField("a", StringType, false)))
  }

  it should "set nullable in schema if Option" in {
    rowFormat[ClassWithOption].schema shouldEqual StructType(Seq(StructField("a", StringType, true)))
  }
  
  it should "generate a RowFormat for a case class with nested case classes" in {
    val format = rowFormat[NestedClass]
    format.write(NestedClass("a", FlatClass("a"))) shouldEqual Row("a", Row("a"))
    format.read(Row("a", Row("a"))) shouldEqual NestedClass("a", FlatClass("a"))
    format.schema shouldEqual StructType(Seq(StructField("a", StringType, false), StructField("b", StructType(Seq(StructField("a", StringType, false))), false)))
  }

  it should "throw RowDeserializationException if Row types are invalid" in {
    val format = rowFormat[FlatClass]
    intercept[RowDeserializationException](format.read(Row(1)))
  }

  it should "throw RowDeserializationException if Row has wrong number of fields" in {
    val format = rowFormat[FlatClass]
    intercept[RowDeserializationException](format.read(Row()))
  }

  "FamilyFormats" should "be serializable" in {
    new ObjectOutputStream(new NullOutputStream).writeObject(new FamilyFormats {})
  }

  "GeneratedFormats" should "be serializable" in {
    implicit val instantFormat: ColumnFormat[Instant] = CustomColumnFormat[Instant, Timestamp](
      instant => new Timestamp(instant.toEpochMilli),
      timestamp => timestamp.toInstant
    )
    new ObjectOutputStream(new NullOutputStream).writeObject(rowFormat[NestedClassWithCustomType])
  }
}

case class FlatClass(a: String)
case class NestedClass(a: String, b: FlatClass)
case class ClassWithOption(a: Option[String])
case class NestedClassWithCustomType(a: String, b: FlatClass, c: Seq[String], d: Array[String], e: Map[String, String], f: Instant)

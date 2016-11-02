# spark-sql-formats

This is a scala library providing an alternative to Spark Dataset Encoders as a way to convert arbitrary case classes to a structured Row with minimal boiler-plate and compile-time safety using Shapeless.

It was inspired by:
* https://github.com/spray/spray-json
* https://github.com/fommil/spray-json-shapeless

# Motivation

* Dataset Encoders are a complicated black box that is difficult to understand and debug
    * They make heavy use of reflection and code generation
    * There are numerous bugs with no workarounds. E.g. task serialization
    * They don't provide simple ways to support custom types
    * Errors are not caught at compile time
* Provide a pluggable type class approach so that marshalling strategies can be interchanged
* Simple way of supporting custom types

# Examples

## Generate a format for a case class

```scala
// Auto generating a format using out of the box formats requires the following imports
import com.github.upio.spark.sql.ColumnFormats._ // individual strategies for marshalling a Type to a Spark SQL Column
import com.github.upio.spark.sql.FamilyFormats._ // implicit generators for creating a RowFormat for an arbitrary case class

case class MyClass(a: String)
implicit val myClassFormat = rowFormat[MyClass]
```

## Convert a case class to/from Row

```scala
val myClass: MyClass = implicitly[RowFormat[MyClass]].read(row)
val myClass: MyClass = rowFormat[MyClass].read(row)

val row: Row = implicitly[RowFormat[MyClass]].write(myClass)
val row: Row = rowFormat[MyClass].write(myClass)

// Use implicits to remove boiler-plate
import com.github.upio.spark.sql.Implicits._
val myClass: MyClass = row.convertTo[MyClass]
val row: Row = myClass.toRow
```

## Create a DataFrame from a RDD[MyClass]

```scala
val format = rowFormat[MyClass]
val rdd: RDD[MyClass] = _

sqlContext.createDataFrame(rdd.map(format.write), format.schema)
sqlContext.createDataFrame(rdd.map(_.toRow), rowFormat[MyClass].schema)
```

## Read a DataFrame to a RDD[MyClass]

```scala
val format = rowFormat[MyClass]
val df: DataFrame = _

df.rdd.map(format.read)
df.rdd.map(_.convertTo[MyClass])
```

## Add support for a custom type

```scala
case class Test(time: Instant)

rowFormat[Test] // does not compile

// Provide a mapping between Instant, the custom type, and Timestamp, the supported type.
implicit val instantFormat: ColumnFormat[Instant] = CustomColumnFormat[Instant, Timestamp](
    instant => new Timestamp(instant.toEpochMilli),
    timestamp => timestamp.toInstant
)
implicit val testFormat = rowFormat[Test] // compiles
```

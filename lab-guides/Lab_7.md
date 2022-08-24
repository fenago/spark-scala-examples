

Spark Load CSV File into RDD
============================

In this tutorial, I will explain how to load a CSV file into Spark RDD
using a Scala example. Using the textFile() the method in
[SparkContext]
class we can read CSV files, multiple CSV files (based on pattern
matching), or all files from a directory into RDD \[String\] object.



Before we start, let's assume we have the following CSV file names with
comma delimited file contents at folder "c:/tmp/files" and I use these
files to demonstrate the examples.

  -----------------------------------------------------------------------
  File Name                           File Contents
  ----------------------------------- -----------------------------------
  text01.csv                          Col1,Col2\
                                      One,1\
                                      Eleven,11

  text02.csv                          Col1,Col2\
                                      Two,2\
                                      Twenty One,21

  text03.csv                          Col1,Col2\
                                      Three,3

  text04.csv                          Col1,Col2\
                                      Four,4

  invalid.csv                         Col1,Col2\
                                      Invalid,I
  -----------------------------------------------------------------------



Load CSV file into RDD
----------------------------------------------------------------------------------------

textFile() method read an entire CSV record as a String and returns
RDD\[String\], hence, we need to write additional code in Spark to
transform RDD\[String\] to RDD\[Array\[String\]\] by splitting the
string record with a delimiter.



The below example reads a file into "rddFromFile" RDD object, and each
element in RDD represents as a String.

```
  val rddFromFile = spark.sparkContext.textFile("C:/tmp/files/text01.txt")
```



But, we would need every record in a CSV to split by comma delimiter and
store it in RDD as multiple columns, In order to achieve this, we should
use `map()` transformation on RDD where we will convert RDD\[String\] to
RDD\[Array\[String\] by splitting every record by comma delimiter. map()
method returns a new RDD instead of updating existing.

```
 val rdd = rddFromFile.map(f=>{
    f.split(",")
  })
```



Now, read the data from rdd by using `foreach`, since the elements in
RDD are array, we need to use the index to retrieve each element from an
array.



```
  rdd.foreach(f=>{
    println("Col1:"+f(0)+",Col2:"+f(1))
  })
```



Note that the output we get from the above "println" also contains
header names from a CSV file as header considered as data itself in RDD.
We need to skip the header while processing the data. This is where the
DataFrame comes handy to read CSV file with a header and handles a lot
more options and file formats.

```
Col1:col1,Col2:col2
Col1:One,Col2:1
Col1:Eleven,Col2:11
```



Let's see how to collect the data from RDD using `collect()`. In this
case, collect() method returns Array\[Array\[String\]\] type where the
first Array represents the RDD data and inner array is a record.

```
  rdd.collect().foreach(f=>{
    println("Col1:"+f(0)+",Col2:"+f(1))
  })
```



This example also yields the same as the above output.






Skip Header From CSV file
----------------------------------------------------------------------------------------------

When you have a header with column names in a CSV file and to read and
process with Spark RDD, you need to skip the header as there is no way
in RDD to specify your file has a header.

rdd.mapPartitionsWithIndex { (idx, iter) =\> if (idx == 0) iter.drop(1)
else iter }

Read Multiple CSV Files into RDD
------------------------------------------------------------------------------------------------------------

To read multiple CSV files in Spark, just use textFile() method on
[SparkContext]
object by passing all file names comma separated. The below example
reads text01.csv & text02.csv files into single RDD.

```
  val rdd4 = spark.sparkContext.textFile("C:/tmp/files/text01.csv,C:/tmp/files/text02.csv")
  rdd4.foreach(f=>{
    println(f)
  })
```



Read all CSV Files in a Directory into RDD
--------------------------------------------------------------------------------------------------------------------------------

To read all CSV files in a directory or folder, just pass a directory
path to the testFile() method.

```
  val rdd3 = spark.sparkContext.textFile("C:/tmp/files/*")
  rdd3.foreach(f=>{
    println(f)
  })
```



### Complete example

```
package com.sparkbyexamples.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ReadMultipleCSVFiles extends App {

  val spark:SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("sparkexamples")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  println("spark read csv files from a directory into RDD")
  val rddFromFile = spark.sparkContext.textFile("C:/tmp/files/text01.csv")
  println(rddFromFile.getClass)

  val rdd = rddFromFile.map(f=>{
    f.split(",")
  })

  println("Iterate RDD")
  rdd.foreach(f=>{
    println("Col1:"+f(0)+",Col2:"+f(1))
  })
  println(rdd)

  println("Get data Using collect")
  rdd.collect().foreach(f=>{
    println("Col1:"+f(0)+",Col2:"+f(1))
  })

  println("read all csv files from a directory to single RDD")
  val rdd2 = spark.sparkContext.textFile("C:/tmp/files/*")
  rdd2.foreach(f=>{
    println(f)
  })

  println("read csv files base on wildcard character")
  val rdd3 = spark.sparkContext.textFile("C:/tmp/files/text*.csv")
  rdd3.foreach(f=>{
    println(f)
  })

  println("read multiple csv files into a RDD")
  val rdd4 = spark.sparkContext.textFile("C:/tmp/files/text01.csv,C:/tmp/files/text02.csv")
  rdd4.foreach(f=>{
    println(f)
  })

}
```



This complete example can be downloaded from [GitHub
project](https://github.com/sparkbyexamples/spark-examples/blob/bfef54903032637b3b8d92591c64908fcad15369/spark-sql-examples/src/main/scala/com/sparkbyexamples/spark/rdd/ReadMultipleCSVFiles.scala)

### Conclusion

In this tutorial you have learned how to read a single CSV file,
multiples CSV files and reading all CSV files from a directory/folder
into a single Spark RDD. You have also learned how to skip the header
while reading CSV files into RDD.


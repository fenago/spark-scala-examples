

Spark -- Read multiple text files into single RDD?
==================================================


Spark core provides `textFile()` & `wholeTextFiles()` methods in
[SparkContext]
class which is used to read single and multiple text or csv files into a
single [Spark RDD].
Using this method we can also read all files from a directory and files
with a specific pattern.



textFile() -- Read single or multiple text, csv files and returns a
single Spark RDD \[String\]

wholeTextFiles() -- Reads single or multiple files and returns a single
RDD\[Tuple2\[String, String\]\], where first value (\_1) in a tuple is a
file name and second value (\_2) is content of the file.



In this article let's see some examples with both of these methods using
Scala and PySpark languages.

-   [Read all text files from a directory into a single
    RDD]
-   [Read multiple text files into a single
    RDD]
-   [Read all text files matching a pattern to single
    RDD]
-   [Read files from multiple directories into single
    RDD]
-   [Reading text files from nested directories into Single
    RDD]
-   [Reading all text files separately and union to create a Single
    RDD]
-   [Reading CSV
    files]

Before we start, let's assume we have the following file names and file
contents at folder "c:/tmp/files" and I use these files to demonstrate
the examples.

  File Name     File Contents
  ------------- ---------------
  text01.txt    One,1
  text02.txt    Two,2
  text03.txt    Three,3
  text04.txt    Four,4
  invalid.txt   Invalid,I

Spark Read all text files from a directory into a single RDD
--------------------------------------------------------------------------------------------------------------------------------------------------------------------

In Spark, by inputting path of the directory to the` textFile()` method
reads all text files and creates a single RDD. Make sure you do not have
a nested directory If it finds one Spark process fails with an error.



```
val rdd = spark.sparkContext.textFile("C:/tmp/files/*")
rdd.foreach(f=>{
    println(f)
})
```



This example reads all files from a directory, creates a single RDD and
prints the contents of the RDD.

```
Invalid,I
One,1
Two,2
Three,3
Four,4
```



If you are running on a cluster you should first collect the data in
order to print on a console as shown below.

```
rdd.collect.foreach(f=>{
println(f)
})
```



Let's see a similar example with `wholeTextFiles()` method. note that
this returns an RDD\[Tuple2\]. where first value (\_1) in a tuple is a
file name and second value (\_2) is content of the file.

```
  val rddWhole = spark.sparkContext.wholeTextFiles("C:/tmp/files/*")
  rddWhole.foreach(f=>{
    println(f._1+"=>"+f._2)
  })
```



Yields below output.

```
file:/C:/tmp/files/invalid.txt=>Invalid,I
file:/C:/tmp/files/text01.txt=>One,1
file:/C:/tmp/files/text02.txt=>Two,2
file:/C:/tmp/files/text03.txt=>Three,3
file:/C:/tmp/files/text04.txt=>Four,4
```



Spark Read multiple text files into a single RDD
--------------------------------------------------------------------------------------------------------------------------------------------

When you know the names of the multiple files you would like to read,
just input all file names with comma separator in order to create a
single RDD.

```
  val rdd3 = spark.sparkContext.textFile("C:/tmp/files/text01.txt,C:/tmp/files/text02.txt")
  rdd3.foreach(f=>{
    println(f)
  })
```



This read file text01.txt & text02.txt files and outputs below content.

```
One,1
Two,2
```



Read all text files matching a pattern to single RDD
----------------------------------------------------------------------------------------------------------------------------------------------------

textFile() method also accepts pattern matching and wild characters. For
example below snippet read all files start with text and with the
extension ".txt" and creates single RDD.

```
  val rdd2 = spark.sparkContext.textFile("C:/tmp/files/text*.txt")
  rdd2.foreach(f=>{
    println(f)
  })
```



Yields below output.

```
One,1
Two,2
Three,3
Four,4
```



Read files from multiple directories into single RDD
----------------------------------------------------------------------------------------------------------------------------------------------------

It also supports reading files and multiple directories combination.

```
  val rdd2 = spark.sparkContext.textFile("C:/tmp/dir1/*,C:/tmp/dir2/*,c:/tmp/files/text01.txt")
  rdd2.foreach(f=>{
    println(f)
  })
```



Yields below output

```
One,1
Two,2
Invalid,I
One,1
Two,2
Three,3
Four,4
```



Reading text files from nested directories into Single RDD
----------------------------------------------------------------------------------------------------------------------------------------------------------------

textFile() and wholeTextFile() returns an error when it finds a nested
folder hence, first using scala, Java, Python languages create a file
path list by traversing all nested folders and pass all file names with
comma separator in order to create a single RDD.

Reading all text files separately and union to create a Single RDD
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

You can also read all text files into a separate RDD's and union all
these to create a single RDD.

Reading multiple CSV files into RDD
------------------------------------------------------------------------------------------------------------------

Spark RDD's doesn't have a method to read csv file formats hence we will
use textFile() method to read csv file like any other text file into RDD
and split the record based on comma, pipe or any other delimiter.

```
 val rdd5 = spark.sparkContext.textFile("C:/tmp/files/*")
  val rdd6 = rdd5.map(f=>{
    f.split(",")
  })

  rdd6.foreach(f => {
    println("Col1:"+f(0)+",Col2:"+f(1))
  })
```



Here, we read all csv files in a directory into RDD, we apply map
transformation to split the record on comma delimiter and a map returns
another RDD "rdd6" after transformation. finally, we iterate rdd6, reads
the column based on an index.

Note: You can't update RDD as they are immutable. this example yields
the below output.

```
Col1:Invalid,Col2:I
Col1:One,Col2:1
Col1:Two,Col2:2
Col1:Three,Col2:3
Col1:Four,Col2:4
```



Complete code
----------------------------------------------------------------------

```
package com.sparkbyexamples.spark.rdd

import org.apache.spark.sql.SparkSession

object ReadMultipleFiles extends App {

  val spark:SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("sparkexamples")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  println("read all text files from a directory to single RDD")
  val rdd = spark.sparkContext.textFile("C:/tmp/files/*")
  rdd.foreach(f=>{
    println(f)
  })

  println("read text files base on wildcard character")
  val rdd2 = spark.sparkContext.textFile("C:/tmp/files/text*.txt")
  rdd2.foreach(f=>{
    println(f)
  })

  println("read multiple text files into a RDD")
  val rdd3 = spark.sparkContext.textFile("C:/tmp/files/text01.txt,C:/tmp/files/text02.txt")
  rdd3.foreach(f=>{
    println(f)
  })

  println("Read files and directory together")
  val rdd4 = spark.sparkContext.textFile("C:/tmp/files/text01.txt,C:/tmp/files/text02.txt,C:/tmp/files/*")
  rdd4.foreach(f=>{
    println(f)
  })


  val rddWhole = spark.sparkContext.wholeTextFiles("C:/tmp/files/*")
  rddWhole.foreach(f=>{
    println(f._1+"=>"+f._2)
  })

  val rdd5 = spark.sparkContext.textFile("C:/tmp/files/*")
  val rdd6 = rdd5.map(f=>{
    f.split(",")
  })

  rdd6.foreach(f => {
    println("Col1:"+f(0)+",Col2:"+f(1))
  })

}
```



This complete code is also available on
[GitHub](https://github.com/fenago/spark-scala-examples/blob/master/src/main/scala/com/sparkbyexamples/spark/rdd/ReadMultipleFiles.scala)
for reference.


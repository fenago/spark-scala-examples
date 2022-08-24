

Spark -- How to create an empty RDD?
====================================


We often need to create empty RDD in Spark, and empty RDD can be created
in several ways, for example, with partition, without partition, and
with pair RDD. In this article, we will see these with Scala, Java and
Pyspark examples.



Spark sc.emptyRDD -- Creates empty RDD with no partition
----------------------------------------------------------------------------------------------------------------------------------------------------------

In Spark, using `emptyRDD()` function on the SparkContext object creates
an empty RDD with no partitions or elements. The below examples create
an empty RDD.

```
val spark:SparkSession = SparkSession.builder()
    .master("local[3]")
    .appName("sparkexamples")
    .getOrCreate()
val rdd = spark.sparkContext.emptyRDD // creates EmptyRDD[0]
val rddString = spark.sparkContext.emptyRDD[String] // creates EmptyRDD[1]
println(rdd)
println(rddString)
println("Num of Partitions: "+rdd.getNumPartitions) // returns o partition
```



From the above `spark.sparkContext.emptyRDD` creates an EmptyRDD\[0\]
and `spark.sparkContext.emptyRDD[String]` creates EmptyRDD\[1\] of
String type. And both of these empty RDD's created with 0 partitions.
Statements println() from this example yields below output.



```
EmptyRDD[0] at emptyRDD at CreateEmptyRDD.scala:12
EmptyRDD[1] at emptyRDD at CreateEmptyRDD.scala:13
Num of Partitions: 0
```



Note that writing an empty RDD creates a folder with .\_SUCCESS.crc file
and \_SUCCESS file with zero size.

```
rdd.saveAsTextFile("test.txt")
//outputs
java.io.IOException: (null) entry in command string: null chmod 0644
```



Once we have empty RDD, we can easily [create an empty
DataFrame]
from rdd object.

Create an Empty RDD with Partition
----------------------------------------------------------------------------------------------------------------

Using Spark sc.parallelize() we can create an empty RDD with partitions,
writing partitioned RDD to a file results in the creation of multiple
part files.



```
  val rdd2 = spark.sparkContext.parallelize(Seq.empty[String])
  println(rdd2)
  println("Num of Partitions: "+rdd2.getNumPartitions)
```



From the above `spark.sparkContext.parallelize(Seq.empty[String])`
creates an `ParallelCollectionRDD[2]` with 3 partitions.

```
ParallelCollectionRDD[2] at parallelize at CreateEmptyRDD.scala:21
Num of Partitions: 3
```



Here is another example using `sc.parallelize()`

```
val emptyRDD = sc.parallelize(Seq(""))
```



Creating an Empty pair RDD
------------------------------------------------------------------------------------------------

Most we use RDD with pair hence, here is another example of creating an
RDD with pair. This example creates an empty RDD with String & Int pair.

```
type pairRDD = (String,Int)
var resultRDD = sparkContext.emptyRDD[pairRDD]
```



Yields below output.

```
EmptyRDD[3] at emptyRDD at CreateEmptyRDD.scala:30
```



Java -- creating an empty RDD
-----------------------------------------------------------------------------------------------------

Similar to Scala, In Java also we can create an empty RDD by call
`emptyRDD()` function on JavaSparkContext object.


```
JavaSparkContext jsc;
// create java spark context and assign it to jsc object.
JavaRDD<T> emptyRDD = jsc.emptyRDD();
```



PySpark -- creating an empty RDD
-----------------------------------------------------------------------------------------------------------

Complete example in Scala
----------------------------------------------------------------------------------------------

The complete code can also refer from
[GitHub](https://github.com/sparkbyexamples/spark-examples/blob/master/spark-sql-examples/src/main/scala/com/sparkbyexamples/spark/rdd/CreateEmptyRDD.scala)
project

```
package com.sparkbyexamples.spark.rdd

import org.apache.spark.sql.SparkSession

object CreateEmptyRDD extends App{

  val spark:SparkSession = SparkSession.builder()
    .master("local[3]")
    .appName("sparkexamples")
    .getOrCreate()

  val rdd = spark.sparkContext.emptyRDD // creates EmptyRDD[0]
  val rddString = spark.sparkContext.emptyRDD[String] // creates EmptyRDD[1]

  println(rdd)
  println(rddString)
  println("Num of Partitions: "+rdd.getNumPartitions) // returns o partition

  //rddString.saveAsTextFile("test.txt") 

  val rdd2 = spark.sparkContext.parallelize(Seq.empty[String])
  println(rdd2)
  println("Num of Partitions: "+rdd2.getNumPartitions)

  //rdd2.saveAsTextFile("test2.txt")

  // Pair RDD

  type dataType = (String,Int)
  var pairRDD = spark.sparkContext.emptyRDD[dataType]
  println(pairRDD)

}
```



In this article, you have learned how to create an empty RDD in Spark
with partition, no partition and finally with pair RDD. Hope it helps
you.


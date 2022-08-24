

Spark Repartition() vs Coalesce()
=================================



Spark `repartition()` vs `coalesce()` -- repartition() is used to
increase or decrease the RDD, DataFrame, Dataset partitions whereas the
coalesce() is used to only decrease the number of partitions in an
efficient way.



One important point to note is, Spark `repartition()` and` coalesce()`
are very expensive operations as they shuffle the data across many
partitions hence try to minimize repartition as much as possible.



1. Spark RDD repartition() vs coalesce()
-----------------------------------------------------------------------------------------------------------------------

In RDD, you can create parallelism at the time of the [creation of an
RDD]()
using
[parallelize()],
[textFile()]
and
[wholeTextFiles()].
You can download the
[test.txt](https://github.com/fenago/spark-scala-examples/blob/master/src/main/resources/test.txt)
file used in this example from GitHub.

```
  val spark:SparkSession = SparkSession.builder()
    .master("local[5]")
    .appName("sparkexamples")
    .getOrCreate()

  val rdd = spark.sparkContext.parallelize(Range(0,20))
  println("From local[5]"+rdd.partitions.size)

  val rdd1 = spark.sparkContext.parallelize(Range(0,25), 6)
  println("parallelize : "+rdd1.partitions.size)

  val rddFromFile = spark.sparkContext.textFile("src/main/resources/test.txt",10)
  println("TextFile : "+rddFromFile.partitions.size)
```



The above example yields below output

```
From local[5] : 5
Parallelize : 6
TextFile : 10
```



`spark.sparkContext.parallelize(Range(0,20),6)` distributes RDD into 6
partitions and the data is distributed as below.



```
rdd1.saveAsTextFile("/tmp/partition")
//Writes 6 part files, one for each partition
Partition 1 : 0 1 2
Partition 2 : 3 4 5
Partition 3 : 6 7 8 9
Partition 4 : 10 11 12
Partition 5 : 13 14 15
Partition 6 : 16 17 18 19
```



### 1.1 RDD repartition()

Spark RDD repartition() method is used to increase or decrease the
partitions. The below example decreases the partitions from 10 to 4 by
moving data from all partitions.

```
  val rdd2 = rdd1.repartition(4)
  println("Repartition size : "+rdd2.partitions.size)
  rdd2.saveAsTextFile("/tmp/re-partition")
```



This yields output `Repartition size : 4` and the repartition
re-distributes the data(as shown below) from all partitions which is
full shuffle leading to very expensive operation when dealing with
billions and trillions of data.

```
Partition 1 : 1 6 10 15 19
Partition 2 : 2 3 7 11 16
Partition 3 : 4 8 12 13 17
Partition 4 : 0 5 9 14 18
```








### 1.2 RDD coalesce()

Spark RDD `coalesce()` is used only to reduce the number of partitions.
This is optimized or improved version of `repartition()` where the
movement of the data across the partitions is lower using coalesce.

```
  val rdd3 = rdd1.coalesce(4)
  println("Repartition size : "+rdd3.partitions.size)
  rdd3.saveAsTextFile("/tmp/coalesce")
```



If you compared the below output with section 1, you will notice
partition 3 has been moved to 2 and Partition 6 has moved to 5,
resulting data movement from just 2 partitions.

```
Partition 1 : 0 1 2
Partition 2 : 3 4 5 6 7 8 9
Partition 4 : 10 11 12 
Partition 5 : 13 14 15 16 17 18 19
```



### 1.3 Complete Example of Spark RDD repartition and coalesce

Below is complete example of Spark RDD repartition and coalesce in Scala
language.

```
package com.sparkbyexamples.spark.rdd

import org.apache.spark.sql.SparkSession

object RDDRepartitionExample extends App {

  val spark:SparkSession = SparkSession.builder()
    .master("local[5]")
    .appName("sparkexamples")
    .getOrCreate()

  val rdd = spark.sparkContext.parallelize(Range(0,20))
  println("From local[5]"+rdd.partitions.size)

  val rdd1 = spark.sparkContext.parallelize(Range(0,20), 6)
  println("parallelize : "+rdd1.partitions.size)

  rdd1.partitions.foreach(f=> f.toString)
  val rddFromFile = spark.sparkContext.textFile("src/main/resources/test.txt",9)

  println("TextFile : "+rddFromFile.partitions.size)

  rdd1.saveAsTextFile("c:/tmp/partition")
  val rdd2 = rdd1.repartition(4)
  println("Repartition size : "+rdd2.partitions.size)

  rdd2.saveAsTextFile("c:/tmp/re-partition")

  val rdd3 = rdd1.coalesce(4)
  println("Repartition size : "+rdd3.partitions.size)

  rdd3.saveAsTextFile("c:/tmp/coalesce")
}
```



2. Spark DataFrame repartition() vs coalesce()
-----------------------------------------------------------------------------------------------------------------------------------

Unlike RDD, you can't specify the partition/parallelism while [creating
DataFrame].
DataFrame or Dataset by default uses the methods specified in Section 1
to determine the default partition and splits the data for parallelism.

```
  val spark:SparkSession = SparkSession.builder()
    .master("local[5]")
    .appName("sparkexamples")
    .getOrCreate()

 val df = spark.range(0,20)
 println(df.rdd.partitions.length)

 df.write.mode(SaveMode.Overwrite)csv("partition.csv")
```



The above example creates 5 partitions as specified in
`master("local[5]")` and the data is distributed across all these 5
partitions.

```
Partition 1 : 0 1 2 3
Partition 2 : 4 5 6 7
Partition 3 : 8 9 10 11
Partition 4 : 12 13 14 15
Partition 5 : 16 17 18 19
```



### 2.1 DataFrame repartition()

Similar to RDD, the Spark DataFrame repartition() method is used to
increase or decrease the partitions. The below example increases the
partitions from 5 to 6 by moving data from all partitions.

```
val df2 = df.repartition(6)
println(df2.rdd.partitions.length)
```



Just increasing 1 partition results data movements from all partitions.

```
Partition 1 : 14 1 5
Partition 2 : 4 16 15
Partition 3 : 8 3 18
Partition 4 : 12 2 19
Partition 5 : 6 17 7 0
Partition 6 : 9 10 11 13
```



And, even decreasing the partitions also results in moving data from all
partitions. hence when you wanted to decrease the partition
recommendation is to use coalesce()/

### 2.2 DataFrame coalesce()

Spark DataFrame `coalesce()` is used only to decrease the number of
partitions. This is an optimized or improved version of repartition()
where the movement of the data across the partitions is fewer using
coalesce.

```
 val df3 = df.coalesce(2)
 println(df3.rdd.partitions.length)
```



This yields output 2 and the resultant partition looks like

```
Partition 1 : 0 1 2 3 8 9 10 11
Partition 2 : 4 5 6 7 12 13 14 15 16 17 18 19
```



Since we are reducing 5 to 2 partitions, the data movement happens only
from 3 partitions and it moves to remain 2 partitions.

### Default Shuffle Partition

Calling `groupBy()`, `union()`, `join()` and similar functions on
DataFrame results in shuffling data between multiple executors and even
machines and finally repartitions data into **200 partitions by
default**. Spark default defines shuffling partition to 200 using
`spark.sql.shuffle.partitions` configuration.

```
 val df4 = df.groupBy("id").count()
 println(df4.rdd.getNumPartitions)
```



#### Conclusion

In this Spark repartition and coalesce article, you have learned how to
create an RDD with partition, repartition the RDD & DataFrame using
repartition() and coalesce() methods, and learned the difference between
repartition and coalesce.

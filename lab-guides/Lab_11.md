
Spark Pair RDD Functions
========================



Spark defines **PairRDDFunctions** class with several functions to work
with Pair RDD or RDD key-value pair, In this tutorial, we will learn
these functions with Scala examples. Pair RDD's are come in handy when
you need to apply transformations like hash partition, set operations,
joins e.t.c.



All these functions are grouped into Transformations and Actions similar
to regular RDD's.

Spark Pair RDD Transformation Functions
--------------------------------------------------------------------------------------------------------------------------

  Pair RDD Functions         Function Description
  -------------------------- --------------------------------------------------------------------------------------------------------------------------------
  aggregateByKey             Aggregate the values of each key in a data set. This function can return a different result type then the values in input RDD.
  combineByKey               Combines the elements for each key.
  combineByKeyWithClassTag   Combines the elements for each key.
  flatMapValues              It\'s flatten the values of each key with out changing key values and keeps the original RDD partition.
  foldByKey                  Merges the values of each key.
  groupByKey                 Returns the grouped RDD by grouping the values of each key.
  mapValues                  It applied a map function for each value in a pair RDD with out changing keys.
  reduceByKey                Returns a merged RDD by merging the values of each key.
  reduceByKeyLocally         Returns a merged RDD by merging the values of each key and final result will be sent to the master.
  sampleByKey                Returns the subset of the RDD.
  subtractByKey              Return an RDD with the pairs from this whose keys are not in other.
  keys                       Returns all keys of this RDD as a RDD\[T\].
  values                     Returns an RDD with just values.
  partitionBy                Returns a new RDD after applying specified partitioner.
  fullOuterJoin              Return RDD after applying fullOuterJoin on current and parameter RDD
  join                       Return RDD after applying join on current and parameter RDD
  leftOuterJoin              Return RDD after applying leftOuterJoin on current and parameter RDD
  rightOuterJoin             Return RDD after applying rightOuterJoin on current and parameter RDD

Spark Pair RDD Actions
----------------------------------------------------------------------------------------

  Pair RDD Action functions   Function Description
  --------------------------- --------------------------------------------------------------------------------------------------------------------------------------------------
  collectAsMap                Returns the pair RDD as a Map to the Spark Master.
  countByKey                  Returns the count of each key elements. This returns the final result to local Map which is your driver.
  countByKeyApprox            Same as countByKey but returns the partial result. This takes a timeout as parameter to specify how long this function to run before returning.
  lookup                      Returns a list of values from RDD for a given input key.
  reduceByKeyLocally          Returns a merged RDD by merging the values of each key and final result will be sent to the master.
  saveAsHadoopDataset         Saves RDD to any hadoop supported file system (HDFS, S3, ElasticSearch, e.t.c), It uses Hadoop JobConf object to save.
  saveAsHadoopFile            Saves RDD to any hadoop supported file system (HDFS, S3, ElasticSearch, e.t.c), It uses Hadoop OutputFormat class to save.
  saveAsNewAPIHadoopDataset   Saves RDD to any hadoop supported file system (HDFS, S3, ElasticSearch, e.t.c) with new Hadoop API, It uses Hadoop Configuration object to save.
  saveAsNewAPIHadoopFile      Saves RDD to any hadoop supported fule system (HDFS, S3, ElasticSearch, e.t.c), It uses new Hadoop API OutputFormat class to save.

Pair RDD Functions Examples
--------------------------------------------------------------------------------------------------

In this section, I will explain Spark pair RDD functions with scala
examples, before we get started let's create a pair RDD.



```
val spark = SparkSession.builder()
   .appName("SparkByExample")
   .master("local")
   .getOrCreate()
 val rdd = spark.sparkContext.parallelize(
      List("Germany India USA","USA India Russia","India Brazil Canada China")
    )
 val wordsRdd = rdd.flatMap(_.split(" "))
 val pairRDD = wordsRdd.map(f=>(f,1))
 pairRDD.foreach(println)
```



This snippet creates a pair RDD by splitting by space on every element
in an RDD, flatten it to form a single word string on each element in
RDD and finally assigns an integer "1" to every word.

```
(Germany,1)
(India,1)
(USA,1)
(USA,1)
(India,1)
(Russia,1)
(India,1)
(Brazil,1)
(Canada,1)
(China,1)
```



### distinct -- Returns distinct keys.

```
pairRDD.distinct().foreach(println)

//Prints below output
(Germany,1)
(India,1)
(Brazil,1)
(China,1)
(USA,1)
(Canada,1)
(Russia,1)
```



### sortByKey -- Transformation returns an RDD after sorting by key

```
    println("Sort by Key ==>")
    val sortRDD = pairRDD.sortByKey()
    sortRDD.foreach(println)
```



Yields below output.

```
Sort by Key ==>
(Brazil,1)
(Canada,1)
(China,1)
(Germany,1)
(India,1)
(India,1)
(India,1)
(Russia,1)
(USA,1)
(USA,1)
```



### reduceByKey -- Transformation returns an RDD after adding value for each key.

Result RDD contains unique keys.



```
    println("Reduce by Key ==>")
    val wordCount = pairRDD.reduceByKey((a,b)=>a+b)
    wordCount.foreach(println)
```



This reduces the key by summing the values. Yields below output.

```
Reduce by Key ==>
(Brazil,1)
(Canada,1)
(China,1)
(USA,2)
(Germany,1)
(Russia,1)
(India,3)
```



### aggregateByKey -- Transformation same as reduceByKey

In our example, this is similar to reduceByKey but uses a different
approach.

```
    def param1= (accu:Int,v:Int) => accu + v
    def param2= (accu1:Int,accu2:Int) => accu1 + accu2
    println("Aggregate by Key ==> wordcount")
    val wordCount2 = pairRDD.aggregateByKey(0)(param1,param2)
    wordCount2.foreach(println)
```



This example yields the same output as reduceByKey example.






### keys -- Return RDD\[K\] with all keys in an dataset

```
    println("Keys ==>")
    wordCount2.keys.foreach(println)
```



Yields below output

```
Brazil
Canada
China
USA
Germany
Russia
India
```



### values -- return RDD\[V\] with all values in an dataset

```
    println("Keys ==>")
    wordCount2.keys.foreach(println)
```



### count -- This is an action function and returns a count of a dataset

```
println("Count :"+wordCount2.count())
```



### collectAsMap -- This is an action function and returns Map to the master for retrieving all date from a dataset.

```
    println("collectAsMap ==>")
    pairRDD.collectAsMap().foreach(println)
```



Yields below output:

```
(Brazil,1)
(Canada,1)
(Germany,1)
(China,1)
(Russia,1)
(India,1)
```



Complete Example
----------------------------------------------------------------------------

This example is also available at [GitHub
project](https://github.com/fenago/spark-scala-examples/blob/master/src/main/scala/com/sparkbyexamples/spark/rdd/OperationsOnPairRDD.scala)

```
package com.sparkbyexamples.spark.rdd

import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object OperationsOnPairRDD {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SparkByExample")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val rdd = spark.sparkContext.parallelize(
      List("Germany India USA","USA India Russia","India Brazil Canada China")
    )

    val wordsRdd = rdd.flatMap(_.split(" "))
    val pairRDD = wordsRdd.map(f=>(f,1))
    pairRDD.foreach(println)

    println("Distinct ==>")
    pairRDD.distinct().foreach(println)


    //SortByKey
    println("Sort by Key ==>")
    val sortRDD = pairRDD.sortByKey()
    sortRDD.foreach(println)

    //reduceByKey
    println("Reduce by Key ==>")
    val wordCount = pairRDD.reduceByKey((a,b)=>a+b)
    wordCount.foreach(println)

    def param1= (accu:Int,v:Int) => accu + v
    def param2= (accu1:Int,accu2:Int) => accu1 + accu2
    println("Aggregate by Key ==> wordcount")
    val wordCount2 = pairRDD.aggregateByKey(0)(param1,param2)
    wordCount2.foreach(println)

    //keys
    println("Keys ==>")
    wordCount2.keys.foreach(println)

    //values
    println("values ==>")
    wordCount2.values.foreach(println)

    println("Count :"+wordCount2.count())

    println("collectAsMap ==>")
    pairRDD.collectAsMap().foreach(println)

  }
}
```



#### Conclusion:

In this tutorial, you have learned **PairRDDFunctions** class and Spark
Pair RDD transformations & action functions with scala examples.


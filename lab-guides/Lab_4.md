
Create a Spark RDD using Parallelize
====================================


Let's see how to create Spark RDD using parallelize with
`sparkContext.parallelize()` method and using Spark shell and Scala
example.

Before we start let me explain what is RDD, [Resilient Distributed
Datasets (**RDD**)] is
a fundamental data structure of Spark, It is an immutable distributed
collection of objects. Each dataset in **RDD** is divided into logical
partitions, which may be computed on different nodes of the cluster.



-   Spark Parallelizing an existing collection in your driver program

Below is an example of how to create an RDD using a parallelize method
from
[Sparkcontext].
`sparkContext.parallelize(Array(1,2,3,4,5,6,7,8,9,10))` creates an RDD
with an Array of Integers.

**Using sc.parallelize on Spark Shell or REPL**
-----------------------------------------------

Spark shell provides
[SparkContext]
variable "sc", use `sc.parallelize()` to create an RDD.




```
scala> val rdd = sc.parallelize(Array(1,2,3,4,5,6,7,8,9,10))
rdd: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[1] at parallelize at
:24
```



**Using Spark sparkContext.parallelize** in Scala
-------------------------------------------------

If you are using scala, get
[SparkContext]
object from
[SparkSession]
and use `sparkContext.parallelize()` to create rdd, this function also
has another signature which additionally takes integer argument to
specifies the number of partitions. Partitions are basic units of
parallelism in Apache Spark. RDDs in Apache Spark are a collection of
partitions.

```
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object RDDParallelize {

  def main(args: Array[String]): Unit = {
      val spark:SparkSession = SparkSession.builder().master("local[1]")
          .appName("sparkexamples")
          .getOrCreate()
      val rdd:RDD[Int] = spark.sparkContext.parallelize(List(1,2,3,4,5))
      val rddCollect:Array[Int] = rdd.collect()
      println("Number of Partitions: "+rdd.getNumPartitions)
      println("Action: First element: "+rdd.first())
      println("Action: RDD converted to Array[Int] : ")
      rddCollect.foreach(println)
  }
}
```



By executing the above program you should see below output.


```
Number of Partitions: 1
Action: First element: 1
Action: RDD converted to Array[Int] : 
1
2
3
4
5
```



create empty RDD by using **sparkContext.parallelize**
------------------------------------------------------

```
sparkContext.parallelize(Seq.empty[String])
```



The complete code can be downloaded from [GitHub -- Spark Scala Examples
project](https://github.com/fenago/spark-scala-examples/tree/master/src/main/scala/com/sparkbyexamples/spark/rdd/RDDFromParallelizeRange.scala)


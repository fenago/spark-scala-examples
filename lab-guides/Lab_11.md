

Spark RDD Actions with examples
===============================


RDD actions are operations that return the raw values, In other words,
any RDD function that returns other than RDD\[T\] is considered as an
action in spark programming. In this tutorial, we will learn RDD actions
with Scala examples.



As mentioned in [RDD Transformations],
all transformations are lazy meaning they do not get executed right away
and action functions trigger to execute the transformations.


Complete code I've used in this article is available at [GitHub project
for quick
reference](https://github.com/fenago/spark-scala-examples/blob/master/src/main/scala/com/sparkbyexamples/spark/rdd/RDDActions.scala).


RDD Actions Example
----------------------------------------------------------------------------------

Before we start explaining RDD actions with examples, first, let's
create an RDD.



```
  val spark = SparkSession.builder()
    .appName("SparkByExample")
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  val inputRDD = spark.sparkContext.parallelize(List(("Z", 1),("A", 20),("B", 30),("C", 40),("B", 30),("B", 60)))
  
  val listRdd = spark.sparkContext.parallelize(List(1,2,3,4,5,3,2))
```



Note that we have created two RDD's in the above code snippet and we use
these two as and when necessary to demonstrate the RDD actions.

### aggregate -- action

`aggregate()` the elements of each partition, and then the results for
all the partitions.

```
  //aggregate
  def param0= (accu:Int, v:Int) => accu + v
  def param1= (accu1:Int,accu2:Int) => accu1 + accu2
  println("aggregate : "+listRdd.aggregate(0)(param0,param1))
  //Output: aggregate : 20

  //aggregate
  def param3= (accu:Int, v:(String,Int)) => accu + v._2
  def param4= (accu1:Int,accu2:Int) => accu1 + accu2
  println("aggregate : "+inputRDD.aggregate(0)(param3,param4))
  //Output: aggregate : 20
```



### treeAggregate -- action

`treeAggregate()` -- Aggregates the elements of this RDD in a
multi-level tree pattern. The output of this function will be similar to
the aggregate function.

```
  //treeAggregate. This is similar to aggregate
  def param8= (accu:Int, v:Int) => accu + v
  def param9= (accu1:Int,accu2:Int) => accu1 + accu2
  println("treeAggregate : "+listRdd.treeAggregate(0)(param8,param9))
  //Output: treeAggregate : 20
```



### fold -- action

`fold()` -- Aggregate the elements of each partition, and then the
results for all the partitions.

```
  //fold
  println("fold :  "+listRdd.fold(0){ (acc,v) =>
    val sum = acc+v
    sum
  })
  //Output: fold :  20

  println("fold :  "+inputRDD.fold(("Total",0)){(acc:(String,Int),v:(String,Int))=>
    val sum = acc._2 + v._2
    ("Total",sum)
  })
  //Output: fold :  (Total,181)
```



### reduce

`reduce()` -- Reduces the elements of the dataset using the specified
binary operator.

```
  //reduce
  println("reduce : "+listRdd.reduce(_ + _))
  //Output: reduce : 20
  println("reduce alternate : "+listRdd.reduce((x, y) => x + y))
  //Output: reduce alternate : 20
  println("reduce : "+inputRDD.reduce((x, y) => ("Total",x._2 + y._2)))
  //Output: reduce : (Total,181)
```



### treeReduce

`treeReduce()` -- Reduces the elements of this RDD in a multi-level tree
pattern.

```
  //treeReduce. This is similar to reduce
  println("treeReduce : "+listRdd.treeReduce(_ + _))
  //Output: treeReduce : 20
```



### collect

`collect()` -Return the complete dataset as an Array.

```
  //Collect
  val data:Array[Int] = listRdd.collect()
  data.foreach(println)
```



### count, countApprox, countApproxDistinct

`count()` -- Return the count of elements in the dataset.

`countApprox()` -- Return approximate count of elements in the dataset,
this method returns incomplete when execution time meets timeout.

`countApproxDistinct()` -- Return an approximate number of distinct
elements in the dataset.

```
  //count, countApprox, countApproxDistinct
  println("Count : "+listRdd.count)
  //Output: Count : 20
  println("countApprox : "+listRdd.countApprox(1200))
  //Output: countApprox : (final: [7.000, 7.000])
  println("countApproxDistinct : "+listRdd.countApproxDistinct())
  //Output: countApproxDistinct : 5
  println("countApproxDistinct : "+inputRDD.countApproxDistinct())
  //Output: countApproxDistinct : 5
```



### countByValue, countByValueApprox

`countByValue()` -- Return Map\[T,Long\] key representing each unique
value in dataset and value represents count each value present.

`countByValueApprox()` -- Same as countByValue() but returns approximate
result.

```
  //countByValue, countByValueApprox
  println("countByValue :  "+listRdd.countByValue())
  //Output: countByValue :  Map(5 -> 1, 1 -> 1, 2 -> 2, 3 -> 2, 4 -> 1)
  //println(listRdd.countByValueApprox())
```



### first

`first()` -- Return the first element in the dataset.

```
  //first
  println("first :  "+listRdd.first())
  //Output: first :  1
  println("first :  "+inputRDD.first())
  //Output: first :  (Z,1)
```



### top

`top()` -- Return top n elements from the dataset.

Note: Use this method only when the resulting array is small, as all the
data is loaded into the driver's memory.

```
  //top
  println("top : "+listRdd.top(2).mkString(","))
  //Output: take : 5,4
  println("top : "+inputRDD.top(2).mkString(","))
  //Output: take : (Z,1),(C,40)
```



### min

`min()` -- Return the minimum value from the dataset.

```
  //min
  println("min :  "+listRdd.min())
  //Output: min :  1
  println("min :  "+inputRDD.min())
  //Output: min :  (A,20)
```



### max

`max()` -- Return the maximum value from the dataset.

```
  //max
  println("max :  "+listRdd.max())
  //Output: max :  5
  println("max :  "+inputRDD.max())
  //Output: max :  (Z,1)
```



### take, takeOrdered, takeSample

`take()` -- Return the first num elements of the dataset.

`takeOrdered()` -- Return the first num (smallest) elements from the
dataset and this is the opposite of the take() action.\
Note: Use this method only when the resulting array is small, as all the
data is loaded into the driver's memory.

`takeSample()` -- Return the subset of the dataset in an Array.\
Note: Use this method only when the resulting array is small, as all the
data is loaded into the driver's memory.

```
  //take, takeOrdered, takeSample
  println("take : "+listRdd.take(2).mkString(","))
  //Output: take : 1,2
  println("takeOrdered : "+ listRdd.takeOrdered(2).mkString(","))
  //Output: takeOrdered : 1,2
  //println("take : "+listRdd.takeSample())
```



Actions -- Complete example
-------------------------------------------------------------------------------------------------

```
package com.sparkbyexamples.spark.rdd

import com.sparkbyexamples.spark.rdd.OperationOnPairRDDComplex.kv
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object RDDActions extends App {

  val spark = SparkSession.builder()
    .appName("SparkByExample")
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  val inputRDD = spark.sparkContext.parallelize(List(("Z", 1),("A", 20),("B", 30),("C", 40),("B", 30),("B", 60)))

  val listRdd = spark.sparkContext.parallelize(List(1,2,3,4,5,3,2))

  //Collect
  val data:Array[Int] = listRdd.collect()
  data.foreach(println)

  //aggregate
  def param0= (accu:Int, v:Int) => accu + v
  def param1= (accu1:Int,accu2:Int) => accu1 + accu2
  println("aggregate : "+listRdd.aggregate(0)(param0,param1))
  //Output: aggregate : 20

  //aggregate
  def param3= (accu:Int, v:(String,Int)) => accu + v._2
  def param4= (accu1:Int,accu2:Int) => accu1 + accu2
  println("aggregate : "+inputRDD.aggregate(0)(param3,param4))
  //Output: aggregate : 20

  //treeAggregate. This is similar to aggregate
  def param8= (accu:Int, v:Int) => accu + v
  def param9= (accu1:Int,accu2:Int) => accu1 + accu2
  println("treeAggregate : "+listRdd.treeAggregate(0)(param8,param9))
  //Output: treeAggregate : 20

  //fold
  println("fold :  "+listRdd.fold(0){ (acc,v) =>
    val sum = acc+v
    sum
  })
  //Output: fold :  20

  println("fold :  "+inputRDD.fold(("Total",0)){(acc:(String,Int),v:(String,Int))=>
    val sum = acc._2 + v._2
    ("Total",sum)
  })
  //Output: fold :  (Total,181)

  //reduce
  println("reduce : "+listRdd.reduce(_ + _))
  //Output: reduce : 20
  println("reduce alternate : "+listRdd.reduce((x, y) => x + y))
  //Output: reduce alternate : 20
  println("reduce : "+inputRDD.reduce((x, y) => ("Total",x._2 + y._2)))
  //Output: reduce : (Total,181)

  //treeReduce. This is similar to reduce
  println("treeReduce : "+listRdd.treeReduce(_ + _))
  //Output: treeReduce : 20

  //count, countApprox, countApproxDistinct
  println("Count : "+listRdd.count)
  //Output: Count : 20
  println("countApprox : "+listRdd.countApprox(1200))
  //Output: countApprox : (final: [7.000, 7.000])
  println("countApproxDistinct : "+listRdd.countApproxDistinct())
  //Output: countApproxDistinct : 5
  println("countApproxDistinct : "+inputRDD.countApproxDistinct())
  //Output: countApproxDistinct : 5

  //countByValue, countByValueApprox
  println("countByValue :  "+listRdd.countByValue())
  //Output: countByValue :  Map(5 -> 1, 1 -> 1, 2 -> 2, 3 -> 2, 4 -> 1)
  //println(listRdd.countByValueApprox())

  //first
  println("first :  "+listRdd.first())
  //Output: first :  1
  println("first :  "+inputRDD.first())
  //Output: first :  (Z,1)

  //top
  println("top : "+listRdd.top(2).mkString(","))
  //Output: take : 5,4
  println("top : "+inputRDD.top(2).mkString(","))
  //Output: take : (Z,1),(C,40)

  //min
  println("min :  "+listRdd.min())
  //Output: min :  1
  println("min :  "+inputRDD.min())
  //Output: min :  (A,20)

  //max
  println("max :  "+listRdd.max())
  //Output: max :  5
  println("max :  "+inputRDD.max())
  //Output: max :  (Z,1)

  //take, takeOrdered, takeSample
  println("take : "+listRdd.take(2).mkString(","))
  //Output: take : 1,2
  println("takeOrdered : "+ listRdd.takeOrdered(2).mkString(","))
  //Output: takeOrdered : 1,2
  //println("take : "+listRdd.takeSample())

  //toLocalIterator
  //listRdd.toLocalIterator.foreach(println)
  //Output:

}
```



#### Conclusion:

RDD actions are operations that return non-RDD values, since RDD's are
lazy they do not execute the transformation functions until we call
actions. hence, all these functions trigger the transformations to
execute and finally returns the value of the action functions to the
driver program. and In this tutorial, you have also learned several RDD
functions usage and examples in scala language.
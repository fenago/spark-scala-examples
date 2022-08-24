

Spark Accumulators Explained
============================

Spark Accumulators are shared variables which are only "added" through
an associative and commutative operation and are used to perform
counters (Similar to Map-reduce counters) or sum operations



Spark by default supports to create an accumulators of any numeric type
and provide a capability to add custom accumulator types.

Programmers can create following accumulators



-   named accumulators
-   unnamed accumulators

When you create a named accumulator, you can see them on Spark web UI
under the "Accumulator" tab. On this tab, you will see two tables; the
first table "accumulable" -- consists of all named accumulator variables
and their values. And on the second table "Tasks" -- value for each
accumulator modified by a task.

And, unnamed accumulators are not shows on Spark web UI, For all
practical purposes it is suggestable to use named accumulators.

Creating Accumulator variable
------------------------------------------------------------------------------------------------------

Spark by default provides accumulator methods for long, double and
collection types. All these methods are present in
[SparkContext]
class and return `<a href="#LongAccumulator">LongAccumulator</a>`,
`<a href="#DoubleAccumulator">DoubleAccumulator</a>`, and
`<a href="#CollectionAccumulator">CollectionAccumulator</a>`
respectively.



-   Long Accumulator
-   Double Accumulator
-   Collection Accumulator

For example, you can create long accumulator on spark-shell using

```
scala> val accum = sc.longAccumulator("SumAccumulator")
accum: org.apache.spark.util.LongAccumulator = LongAccumulator(id: 0, name: Some(SumAccumulator), value: 0)
```



The above statement creates a named accumulator "SumAccumulator". Now,
Let's see how to add up the elements from an array to this accumulator.

```
scala> sc.parallelize(Array(1, 2, 3)).foreach(x => accum.add(x))
-----
-----
scala> accum.value
res2: Long = 6
```



Each of these accumulator classes has several methods, among these,
`add()` method call from tasks running on the cluster. Tasks can't read
the values from the accumulator and only the driver program can read
accumulators value using the `value()` method.






Long Accumulator
----------------------------------------------------------------------------

longAccumulator() methods from SparkContext returns LongAccumulator

**Syntax**

```
//Long Accumulator
def longAccumulator : org.apache.spark.util.LongAccumulator
def longAccumulator(name : scala.Predef.String) : org.apache.spark.util.LongAccumulator
```



You can create named accumulators for long type using
`SparkContext.longAccumulator(v)` and for unnamed use signature that
doesn't take an argument.

```
  val spark = SparkSession.builder()
    .appName("SparkByExample")
    .master("local")
    .getOrCreate()

  val longAcc = spark.sparkContext.longAccumulator("SumAccumulator")
  
  val rdd = spark.sparkContext.parallelize(Array(1, 2, 3))

  rdd.foreach(x => longAcc.add(x))
  println(longAcc.value)
```



LongAccumulator class provides follwoing methods

-   isZero
-   copy
-   reset
-   add
-   count
-   sum
-   avg
-   merge
-   value

Double Accumulator
--------------------------------------------------------------------------------

For named double type using `SparkContext.doubleAccumulator(v)` and for
unnamed use signature that doesn't take an argument.

S**yntax**

```
//Double Accumulator
def doubleAccumulator : org.apache.spark.util.DoubleAccumulator
def doubleAccumulator(name : scala.Predef.String) : org.apache.spark.util.DoubleAccumulator
```



DoubleAccumulator class also provides methods similar to LongAccumulator

Collection Accumulator
----------------------------------------------------------------------------------------

For named collection type using `SparkContext.collectionAccumulator(v)`
and for unnamed use signature that doesn't take an argument.

**Syntax**

```
//Collection Accumulator
def collectionAccumulator[T] : org.apache.spark.util.CollectionAccumulator[T]
def collectionAccumulator[T](name : scala.Predef.String) : org.apache.spark.util.CollectionAccumulator[T]
```



CollectionAccumulator class provides following methods

-   isZero
-   copyAndReset
-   copy
-   reset
-   add
-   merge
-   value

Note: Each of these accumulator classes has several methods, among
these, `add()` method call from tasks running on the cluster. Tasks
can't read the values from the accumulator and only the driver program
can read accumulators value using the `value()` method.

### Conclusion

In this Spark accumulators shared variable lab, you have learned the
Accumulators are only "added" through an associative and commutative and
operation and are used to perform counters (Similar to Map-reduce
counters) or sum operations and also learned different Accumulator
classes along with their methods.

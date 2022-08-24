

What is SparkContext? Explained
===============================



SparkContext is available since Spark 1.x (JavaSparkContext for Java)
and it used to be an entry point to Spark and PySpark before introducing
[SparkSession]
in 2.0. Creating SparkContext is the first step to use RDD and connect
to Spark Cluster, In this lab, you will learn how to create it using
examples.





**What is SparkContext**

Since Spark 1.x, SparkContext is an entry point to Spark and is defined
in `org.apache.spark` package. It is used to programmatically [create
Spark
RDD](),
[accumulators],
and [broadcast
variables]
on the cluster. Its object *`sc`* is default variable available in
spark-shell and it can be programmatically created using `SparkContext`
class.



***Note that you can create only one active SparkContext per JVM.***
***You should stop() the active SparkContext before creating a new
one.***




![Source: spark.apache.org](./Lab_4_files/image04.png)

The Spark driver program creates and uses SparkContext to connect to the
cluster manager to submit Spark jobs, and know what resource manager
(YARN, Mesos or Standalone) to communicate to. It is the heart of the
Spark application.

**Related:** [How to get current SparkContext & its configurations in
Spark]

1. SparkContext in spark-shell
-------------------------------------------------------------------------------------------------------

Be default Spark shell provides `sc` object which is an instance of
SparkContext class. We can directly use this object where required.



```
scala>>sc.appName
```



Similar to the Spark shell, In most of the tools, notebooks, and Azure
Databricks, the environment itself creates a default SparkContext object
for us to use so you don't have to worry about creating a spark context.

2. Spark 2.X -- Create SparkContext using Scala Program
-------------------------------------------------------------------------------------------------------------------------------------------------------

Since Spark 2.0, we mostly use
[SparkSession]
as most of the methods available in SparkContext are also present in
[SparkSession].
Spark session internally creates the Spark Context and exposes the
`sparkContext` variable to use.

At any given time only one `SparkContext` instance should be active per
JVM. In case you want to create another you should stop existing
SparkContext (using `stop()`) before creating a new one.

```
// Create SparkSession object
import org.apache.spark.sql.SparkSession
object SparkSessionTest extends App {
  val spark = SparkSession.builder()
      .master("local[1]")
      .appName("sparkexamples")
      .getOrCreate();
  println(spark.sparkContext)
  println("Spark App Name : "+spark.sparkContext.appname)
}

// Outputs
//org.apache.spark.SparkContext@2fdf17dc
//Spark App Name : sparkexamples
```



As I explained in the
[SparkSession]
lab, you can create any number of SparkSession objects however, for
all those objects underlying there will be only one SparkContext.

3. Create RDD
---------------------------------------------------------------------

Once you create a Spark Context object, use below to create Spark RDD.

```
// Create RDD
val rdd = spark.sparkContext.range(1, 5)
rdd.collect().foreach(print)

// Create RDD from Text file
val rdd2 = spark.sparkContext.textFile("/src/main/resources/text/alice.txt")
```



4. Stop SparkContext
-----------------------------------------------------------------------------------

You can stop the SparkContext by calling the `stop()` method. As
explained above you can have only one SparkContext per JVM. If you
wanted to create another, you need to shutdown it first by using stop()
method and create a new SparkContext.

```
//SparkContext stop() method
spark.sparkContext.stop()
```



When Spark executes this statement, it logs the message ***INFO
SparkContext: Successfully stopped SparkContext*** to console or to a
log file.

5. Spark 1.X -- Creating SparkContext using Scala Program
-----------------------------------------------------------------------------------------------------------------------------------------------------------

In Spark 1.x, first, you need to create a `SparkConf` instance by
assigning app name and setting master by using the SparkConf static
methods `setAppName()` and `setMaster()` respectively and then pass
SparkConf object as an argument to SparkContext constructor to create
Spark Context.

```
// Create SpakContext
import org.apache.spark.{SparkConf, SparkContext}
val sparkConf = new SparkConf().setAppName("sparkexamples").setMaster("local[1]")
val sparkContext = new SparkContext(sparkConf)
```



SparkContext constructor has been deprecated in 2.0 hence, the
recommendation is to use a static method `getOrCreate()` that internally
creates SparkContext. This function instantiates a SparkContext and
registers it as a singleton object.

```
// Create Spark Context
val sc = SparkContext.getOrCreate(sparkConf)
```



6. SparkContext Commonly Used Methods
---------------------------------------------------------------------------------------------------------------------

`longAccumulator()` -- It creates an accumulator variable of a long data
type. Only a driver can access accumulator variables.

`doubleAccumulator()` -- It creates an accumulator variable of a double
data type. Only a driver can access accumulator variables.

`applicationId` -- Returns a unique ID of a Spark application.

`appName` -- Return an app name that was given when creating
SparkContext

`broadcast` -- read-only variable broadcast to the entire cluster. You
can broadcast a variable to a Spark cluster only once.

`emptyRDD` -- Creates an [empty
RDD]

`getPersistentRDDs` -- Returns all persisted RDDs

`getOrCreate()` -- Creates or returns a SparkContext

`hadoopFile` -- Returns an
[RDD] of a Hadoop file

`master()`--  Returns master that set while creating SparkContext

`newAPIHadoopFile` -- Creates an
[RDD] for a Hadoop file
with a new API InputFormat.

`sequenceFile` -- Get an
[RDD] for a Hadoop
SequenceFile with given key and value types.

`setLogLevel` -- Change log level to debug, info, warn, fatal, and error

`textFile` -- [Reads a text
file]
from HDFS, local or any Hadoop supported file systems and returns an RDD

`union` -- Union two RDDs

`wholeTextFiles` -- [Reads a text file in the
folder]
from HDFS, local or any Hadoop supported file systems and returns an RDD
of Tuple2. First element of the tuple consists file name and the second
element consists context of the text file.

7. SparkContext Example
-----------------------------------------------------------------------------------------

```
package com.sparkbyexamples.spark.stackoverflow

import org.apache.spark.{SparkConf, SparkContext}

object SparkContextOld extends App{

  val conf = new SparkConf().setAppName("sparkexamples").setMaster("local[1]")
  val sparkContext = new SparkContext(conf)
  val rdd = sparkContext.textFile("/src/main/resources/text/alice.txt")

  sparkContext.setLogLevel("ERROR")

  println("First SparkContext:")
  println("APP Name :"+sparkContext.appName);
  println("Deploy Mode :"+sparkContext.deployMode);
  println("Master :"+sparkContext.master);
 // sparkContext.stop()
  
  val conf2 = new SparkConf().setAppName("sparkexamples-2").setMaster("local[1]")
  val sparkContext2 = new SparkContext(conf2)

  println("Second SparkContext:")
  println("APP Name :"+sparkContext2.appName);
  println("Deploy Mode :"+sparkContext2.deployMode);
  println("Master :"+sparkContext2.master);
  
}
```



8. Conclusion
-------------

In this Spark Context lab, you have learned what is SparkContext,
how to create in Spark 1.x and Spark 2.0, and using with few basic
examples.


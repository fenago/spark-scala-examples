

Different ways to create Spark RDD
==================================


Spark RDD can be created in several ways using Scala & Pyspark
languages, for example, It can be created by using
sparkContext.parallelize(), from text file, from another RDD, DataFrame,
and Dataset. Though we have covered most of the examples in Scala here,
the same concept can be used to create RDD in PySpark (Python Spark)



**Resilient Distributed Datasets (RDD**) is the fundamental data
structure of Spark. RDDs are immutable and fault-tolerant in nature. RDD
is just the way of representing Dataset distributed across multiple
nodes in a cluster, which can be operated in parallel. RDDs are called
resilient because they have the ability to always re-compute an RDD when
a node failure.

Note that once we create an RDD, we can easily [create a
DataFrame]
from RDD.



Let's see how to create an RDD in Apache Spark with examples:

-   [Spark create RDD from Seq or List
     (using Parallelize)](#from-parallelize)
-   [Creating an RDD from a text
    file](#from-text)
-   [Creating from another
    RDD](#from-rdd)
-   [Creating from existing DataFrames and
    DataSet](#from-dataframe-dataset)

Spark Create RDD from Seq or List (using Parallelize)
----------------------------------------------------------------------------------------------------------------------------------------------------

RDD's are generally created by parallelized collection i.e. by taking an
existing collection from driver program (scala, python e.t.c) and
passing it to SparkContext's `parallelize()` method. This method is used
only for testing but not in realtime as the entire data will reside on
one node which is not ideal for production.

```
val rdd=spark.sparkContext.parallelize(Seq(("Java", 20000), 
  ("Python", 100000), ("Scala", 3000)))
rdd.foreach(println)
```



Outputs:




```
(Python,100000)
(Scala,3000)
(Java,20000)
```



Create an RDD from a text file
--------------------------------------------------------------------------------------------------------

Mostly for production systems, we create RDD's from files. here will see
how to create an RDD by reading data from a file.

```
val rdd = spark.sparkContext.textFile("/path/textFile.txt")
```



This creates an RDD for which each record represents a line in a file.

If you want to read the entire content of a file as a single record use
wholeTextFiles() method on sparkContext.

```
val rdd2 = spark.sparkContext.wholeTextFiles("/path/textFile.txt")
rdd2.foreach(record=>println("FileName : "+record._1+", FileContents :"+record._2))
```



In this case, each text file is a single record. In this, the name of
the file is the first column and the value of the text file is the
second column.

Creating from another RDD
----------------------------------------------------------------------------------------------

You can use transformations like map, flatmap, filter to create a new
RDD from an existing one.

```
val rdd3 = rdd.map(row=>{(row._1,row._2+100)})
```



Above, creates a new RDD "rdd3" by adding 100 to each record on RDD.
this example outputs below.


```
(Python,100100)
(Scala,3100)
(Java,20100)
```



**From existing DataFrames and DataSet**
------------------------------------------------------------------------------------------------------------------------

To convert DataSet or DataFrame to RDD just use `rdd()` method on any of
these data types.

```
val myRdd2 = spark.range(20).toDF().rdd
```



toDF() creates a DataFrame and by calling rdd on DataFrame returns back
RDD.\

#### Conclusion:

In this article, you have learned creating Spark RDD from list or seq,
text file, from another RDD, DataFrame, and Dataset.


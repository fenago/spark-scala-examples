

Spark SQL Shuffle Partitions
============================



The Spark SQL shuffle is a mechanism for redistributing or
re-partitioning data so that the data is grouped differently across
partitions, based on your data size you may need to reduce or increase
the number of partitions of RDD/DataFrame using
`spark.sql.shuffle.partitions` configuration or through code.



Spark shuffle is a very expensive operation as it moves the data between
executors or even between worker nodes in a cluster so try to avoid it
when possible. When you have a performance issue on Spark jobs, you
should look at the Spark transformations that involve shuffling.

In this tutorial, you will learn what triggers the shuffle on RDD and
DataFrame transformations using scala examples. The same approach also
can be used with PySpark (Spark with Python)



What is Spark Shuffle?
---------------------------------------------------------------------------------------

Shuffling is a mechanism Spark uses to [redistribute the
data]
across different executors and even across machines. Spark shuffling
triggers for transformation operations like `gropByKey()`,
`reducebyKey()`, `join()`, `groupBy()` e.t.c

Spark Shuffle is an expensive operation since it involves the following

-   Disk I/O
-   Involves data serialization and deserialization
-   Network I/O

When [creating an
RDD](),
Spark doesn't necessarily store the data for all keys in a partition
since at the time of creation there is no way we can set the key for the
data set.



Hence, when we run the reduceByKey() operation to aggregate the data on
keys, Spark does the following.

-   Spark first runs m*ap* tasks on all partitions which groups all
    values for a single key.
-   The results of the map tasks are kept in memory.
-   When results do not fit in memory, Spark stores the data on a disk.
-   Spark shuffles the mapped data across partitions, some times it also
    stores the shuffled data into a disk for reuse when it needs to
    recalculate.
-   Run the garbage collection
-   Finally runs reduce tasks on each partition based on key.

Spark RDD Shuffle
------------------------------------------------------------------------------

Spark RDD triggers shuffle for several operations like
`repartition()`,  `groupByKey()`,  `reduceByKey()`,
`cogroup()` and `join()` but not `countByKey()` .

```
val spark:SparkSession = SparkSession.builder()
    .master("local[5]")
    .appName("sparkexamples")
    .getOrCreate()

val sc = spark.sparkContext

val rdd:RDD[String] = sc.textFile("src/main/resources/test.txt")

println("RDD Parition Count :"+rdd.getNumPartitions)
val rdd2 = rdd.flatMap(f=>f.split(" "))
  .map(m=>(m,1))

//ReduceBy transformation
val rdd5 = rdd2.reduceByKey(_ + _)

println("RDD Parition Count :"+rdd5.getNumPartitions)

#Output
RDD Parition Count : 3
RDD Parition Count : 3
```



Both `getNumPartitions` from the above examples return the same number
of partitions. Though `reduceByKey()` triggers data shuffle, it doesn't
change the partition count as RDD's inherit the partition size from
parent RDD.






You may get partition counts different based on your setup and how Spark
creates partitions.

Spark SQL DataFrame Shuffle
--------------------------------------------------------------------------------------------------

Unlike RDD, Spark SQL DataFrame API increases the partitions when the
transformation operation performs shuffling. DataFrame operations that
trigger shufflings are
[join()],
and all [aggregate
functions].

```
import spark.implicits._

val simpleData = Seq(("James","Sales","NY",90000,34,10000),
    ("Michael","Sales","NY",86000,56,20000),
    ("Robert","Sales","CA",81000,30,23000),
    ("Maria","Finance","CA",90000,24,23000),
    ("Raman","Finance","CA",99000,40,24000),
    ("Scott","Finance","NY",83000,36,19000),
    ("Jen","Finance","NY",79000,53,15000),
    ("Jeff","Marketing","CA",80000,25,18000),
    ("Kumar","Marketing","NY",91000,50,21000)
  )
val df = simpleData.toDF("employee_name","department","state","salary","age","bonus")

val df2 = df.groupBy("state").count()

println(df2.rdd.getNumPartitions)
```



This outputs the partition count as 200.

Spark Default Shuffle Partition
----------------------------------------------------------------------------------------------------------

DataFrame increases the partition number to 200 automatically when Spark
operation performs data shuffling (join(), aggregation functions). This
default shuffle partition number comes from Spark SQL
configuration `spark.sql.shuffle.partitions` which is by default set to
`200`.

You can change this default shuffle partition value using
*`conf`* method of the *`SparkSession`* object or using [Spark Submit
Command
Configurations].

```
spark.conf.set("spark.sql.shuffle.partitions",100)
println(df.groupBy("_c0").count().rdd.partitions.length)
```



Shuffle partition size
----------------------------------------------------------------------------------------

Based on your dataset size, number of cores, and memory, Spark shuffling
can benefit or harm your jobs. When you dealing with less amount of
data, you should typically reduce the shuffle partitions otherwise you
will end up with many partitioned files with a fewer number of records
in each partition. which results in running many tasks with lesser data
to process.

On another hand, when you have too much data and have less number of
partitions results in fewer longer running tasks, and sometimes you may
also get out of memory error.

Getting the right size of the shuffle partition is always tricky and
takes many runs with different values to achieve the optimized number.
This is one of the key properties to look for when you have performance
issues on Spark jobs.

### Conclusion

In this article, you have learned what is Spark SQL shuffle, how some
Spark operation triggers re-partition of the data, how to change the
default spark shuffle partition, and finally how to get the right
partition size.



Spark -- Difference between Cache and Persist?
==============================================

Spark *Cache* and *persist* are optimization techniques for iterative
and interactive Spark applications to improve the performance of the
jobs or applications. In this lab, you will learn What is Spark
Caching and Persistence, the difference between `Cache()` and
`Persist()` methods and how to use these two with RDD, DataFrame, and
Dataset with Scala examples.



Though Spark provides computation 100 x times faster than traditional
Map Reduce jobs, If you have not designed the jobs to reuse the
repeating computations you will see degrade in performance when you are
dealing with billions or trillions of data. Hence, we may need to look
at the stages and use optimization techniques as one of the ways to
improve performance.

Spark Cache vs Persist
----------------------------------------------------------------------------------------

Using `cache()` and `persist()` methods, Spark provides an optimization
mechanism to store the intermediate computation of an RDD, DataFrame,
and Dataset so they can be reused in subsequent actions(reusing the RDD,
Dataframe, and Dataset computation result's).



Both caching and persisting are used to save the Spark RDD, Dataframe,
and Dataset's. But, the difference is, RDD cache() method default saves
it to memory (MEMORY\_ONLY) whereas persist() method is used to store it
to the user-defined storage level.

When you persist a dataset, each node stores its partitioned data in
memory and reuses them in other actions on that dataset. And Spark's
persisted data on nodes are fault-tolerant meaning if any partition of a
Dataset is lost, it will automatically be recomputed using the original
transformations that created it.

Advantages for Caching and Persistence
------------------------------------------------------------------------------------------------------------------------

Below are the advantages of using Spark Cache and Persist methods.



**Cost efficient** -- Spark computations are very expensive hence
reusing the computations are used to save cost.

**Time efficient** -- Reusing the repeated computations saves lots of
time.

**Execution time** -- Saves execution time of the job and we can perform
more jobs on the same cluster.






Below I will explain how to use Spark Cache and Persist with DataFrame
or Dataset.

Spark Cache Syntax and Example
--------------------------------------------------------------------------------------------------------

[Spark DataFrame or Dataset
caching]
by default saves it to storage level \``MEMORY_AND_DISK`\` because
recomputing the in-memory columnar representation of the underlying
table is expensive. Note that this is different from the default cache
level of \``RDD.cache()`\` which is '`MEMORY_ONLY`'.

S**yntax**

```
 cache() : Dataset.this.type
```



Spark `cache()` method in Dataset class internally calls `persist()`
method which in turn uses
`sparkSession.sharedState.cacheManager.cacheQuery` to cache the result
set of DataFrame or Dataset. Let's look at an example.

**Example**

```
 val spark:SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("sparkexamples")
    .getOrCreate()
  import spark.implicits._
  val columns = Seq("Seqno","Quote")
  val data = Seq(("1", "Be the change that you wish to see in the world"),
    ("2", "Everyone thinks of changing the world, but no one thinks of changing himself."),
    ("3", "The purpose of our lives is to be happy."))
  val df = data.toDF(columns:_*)

  val dfCache = df.cache()
  dfCache.show(false)
```



Spark Persist Syntax and Example
------------------------------------------------------------------------------------------------------------

Spark persist has two signature first signature doesn't take any
argument which by default saves it to `MEMORY_AND_DISK` storage level
and the second signature which takes `StorageLevel` as an argument to
store it to different storage levels.

S**yntax**

```
1) persist() : Dataset.this.type
2) persist(newLevel : org.apache.spark.storage.StorageLevel) : Dataset.this.type
```



**Example**

```
  val dfPersist = df.persist()
  dfPersist.show(false)
```



Using the second signature you can save DataFrame/Dataset to One of the
storage levels `MEMORY_ONLY`,`MEMORY_AND_DISK`, `MEMORY_ONLY_SER`,
`MEMORY_AND_DISK_SER`, `DISK_ONLY`, `MEMORY_ONLY_2`,`MEMORY_AND_DISK_2`

```
  val dfPersist = df.persist(StorageLevel.MEMORY_ONLY)
  dfPersist.show(false)
```



This stores DataFrame/Dataset into Memory.

Unpersist syntax and Example
----------------------------------------------------------------------------------------------------

We can also unpersist the persistence DataFrame or Dataset to remove
from the memory or storage.

S**yntax**

```
unpersist() : Dataset.this.type
unpersist(blocking : scala.Boolean) : Dataset.this.type
```



E**xample**

```
  val dfPersist = dfPersist.unpersist()
  dfPersist.show(false)
```



unpersist(Boolean) with boolean as argument blocks until all blocks are
deleted.

Spark Persistance storage levels
------------------------------------------------------------------------------------------------------------

All different storage level Spark supports are available
at `org.apache.spark.storage.StorageLevel` class. The storage level
specifies how and where to persist or cache a Spark DataFrame and
Dataset.

`MEMORY_ONLY` -- This is the default behavior of the
RDD `cache()` method and stores the RDD or DataFrame as deserialized
objects to JVM memory. When there is no enough memory available it will
not save DataFrame of some partitions and these will be re-computed as
and when required. This takes more memory. but unlike RDD, this would be
slower than **MEMORY\_AND\_DISK** level as it recomputes the unsaved
partitions and recomputing the in-memory columnar representation of the
underlying table is expensive

`MEMORY_ONLY_SER` -- This is the same as `MEMORY_ONLY` but the
difference being it stores RDD as serialized objects to JVM memory. It
takes lesser memory (space-efficient) then MEMORY\_ONLY as it saves
objects as serialized and takes an additional few more CPU cycles in
order to deserialize.

`MEMORY_ONLY_2` -- Same as `MEMORY_ONLY` storage level but replicate
each partition to two cluster nodes.

`MEMORY_ONLY_SER_2` -- Same as `MEMORY_ONLY_SER` storage level but
replicate each partition to two cluster nodes.

`MEMORY_AND_DISK` -- This is the default behavior of the DataFrame or
Dataset. In this Storage Level, The DataFrame will be stored in JVM
memory as a deserialized object. When required storage is greater than
available memory, it stores some of the excess partitions into the disk
and reads the data from the disk when required. It is slower as there is
I/O involved.

`MEMORY_AND_DISK_SER` -- This is the same as `MEMORY_AND_DISK` storage
level difference being it serializes the DataFrame objects in memory and
on disk when space is not available.

`MEMORY_AND_DISK_2` -- Same as `MEMORY_AND_DISK` storage level but
replicate each partition to two cluster nodes.

`MEMORY_AND_DISK_SER_2` -- Same as `MEMORY_AND_DISK_SER` storage level
but replicate each partition to two cluster nodes.

`DISK_ONLY` -- In this storage level, DataFrame is stored only on disk
and the CPU computation time is high as I/O is involved.

`DISK_ONLY_2` -- Same as `DISK_ONLY` storage level but replicate each
partition to two cluster nodes.

Below are the table representation of the Storage level, Go through the
impact of space, cpu and performance choose the one that best fits for
you.

```
Storage Level    Space used  CPU time  In memory  On-disk  Serialized   Recompute some partitions
----------------------------------------------------------------------------------------------------
MEMORY_ONLY          High        Low       Y          N        N         Y    
MEMORY_ONLY_SER      Low         High      Y          N        Y         Y
MEMORY_AND_DISK      High        Medium    Some       Some     Some      N
MEMORY_AND_DISK_SER  Low         High      Some       Some     Y         N
DISK_ONLY            Low         High      N          Y        Y         N
```



Some Points to note on Persistence
----------------------------------------------------------------------------------------------------------------

-   Spark automatically monitors every persist() and cache() calls you
    make and it checks usage on each node and drops persisted data if
    not used or using least-recently-used (LRU) algorithm. As discussed
    in one of the above section you can also manually remove using
    `unpersist()` method.
-   Spark caching and persistence is just one of the optimization
    techniques to improve the performance of Spark jobs.
-   For RDD cache() default storage level is '`MEMORY_ONLY`' but, for
    DataFrame and Dataset, default is '`MEMORY_AND_DISK`'
-   On Spark UI, the Storage tab shows where partitions exist in memory
    or disk across the cluster.
-   Dataset `cache()` is an alias for
    `persist(StorageLevel.MEMORY_AND_DISK)`
-   Caching of Spark DataFrame or Dataset is a lazy operation, meaning a
    DataFrame will not be cached until you trigger an action. 

Conclusion
----------

In this lab, you have learned Spark cache and Persist methods are
optimization techniques to save interim computation results and use them
subsequently and learned what is the difference between Spark Cache and
Persist and finally saw their syntaxes and usages with Scala examples.

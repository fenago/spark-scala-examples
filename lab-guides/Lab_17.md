

Spark Persistence Storage Levels
================================



All different persistence (persist() method) storage level Spark/PySpark
supports are available at `org.apache.spark.storage.StorageLevel` and
`pyspark.StorageLevel` classes respectively. The storage level specifies
how and where to persist or cache a Spark/PySpark RDD, DataFrame, and
Dataset.



All these Storage levels are passed as an argument to the persist()
method of the Spark/Pyspark RDD, DataFrame, and Dataset.

F**or example**



```
import org.apache.spark.storage.StorageLevel
val rdd2 = rdd.persist(StorageLevel.MEMORY_ONLY_SER)
or 
val df2 = df.persist(StorageLevel.MEMORY_ONLY_SER)
```



Here, I will describe all storage levels available in Spark.

Memory only Storage level
----------------------------------------------------------------------------------------------

`StorageLevel.MEMORY_ONLY` is the default behavior of the
RDD `cache()` method
and stores the RDD or DataFrame as deserialized objects to JVM memory.
When there is not enough memory available it will not save DataFrame of
some partitions and these will be re-computed as and when required.

This takes more memory. but unlike RDD, this would be slower
than **MEMORY\_AND\_DISK** level as it recomputes the unsaved
partitions, and recomputing the in-memory columnar representation of the
underlying table is expensive.



Serialize in Memory
----------------------------------------------------------------------------------

`StorageLevel.MEMORY_ONLY_SER` is the same as `MEMORY_ONLY` but the
difference being it stores RDD as serialized objects to JVM memory. It
takes lesser memory (space-efficient) than MEMORY\_ONLY as it saves
objects as serialized and takes an additional few more CPU cycles in
order to deserialize.

Memory only and Replicate
----------------------------------------------------------------------------------------------

`StorageLevel.MEMORY_ONLY_2` is same as `MEMORY_ONLY` storage level but
replicate each partition to two cluster nodes.






Serialized in Memory and Replicate
----------------------------------------------------------------------------------------------------------------

`StorageLevel.MEMORY_ONLY_SER_2` is same as `MEMORY_ONLY_SER` storage
level but replicate each partition to two cluster nodes.

Memory and Disk Storage level
------------------------------------------------------------------------------------------------------

`StorageLevel.MEMORY_AND_DISK` is the default behavior of the DataFrame
or Dataset. In this Storage Level, The DataFrame will be stored in JVM
memory as deserialized objects. When required storage is greater than
available memory, it stores some of the excess partitions into a disk
and reads the data from the disk when required. It is slower as there is
I/O involved.

Serialize in Memory and Disk
----------------------------------------------------------------------------------------------------

`StorageLevel.MEMORY_AND_DISK_SER` is same as `MEMORY_AND_DISK` storage
level difference being it serializes the DataFrame objects in memory and
on disk when space is not available.

Memory, Disk and Replicate
-----------------------------------------------------------------------------------------------

`StorageLevel.MEMORY_AND_DISK_2` is Same as `MEMORY_AND_DISK` storage
level but replicate each partition to two cluster nodes.

Serialize in Memory, Disk and Replicate
-------------------------------------------------------------------------------------------------------------------------

`StorageLevel.MEMORY_AND_DISK_SER_2` is same
as `MEMORY_AND_DISK_SER` storage level but replicate each partition to
two cluster nodes.

Disk only storage level
------------------------------------------------------------------------------------------

In `StorageLevel.DISK_ONLY` storage level, DataFrame is stored only on
disk and the CPU computation time is high as I/O involved.

Disk only and Replicate
------------------------------------------------------------------------------------------

`StorageLevel.DISK_ONLY_2` is same as `DISK_ONLY` storage level but
replicate each partition to two cluster nodes.

When to use what?
-----------------------------------------------------------------------------

Below is the table representation of the Storage level, Go through the
impact of space, CPU, and performance choose the one that best fits you.

```
Storage Level    Space used  CPU time  In memory  On-disk  Serialized   Recompute some partitions
----------------------------------------------------------------------------------------------------
MEMORY_ONLY          High        Low       Y          N        N         Y    
MEMORY_ONLY_SER      Low         High      Y          N        Y         Y
MEMORY_AND_DISK      High        Medium    Some       Some     Some      N
MEMORY_AND_DISK_SER  Low         High      Some       Some     Y         N
DISK_ONLY            Low         High      N          Y        Y         N
```






Spark Read ORC file into DataFrame
==================================





Spark natively supports ORC data source to read ORC into DataFrame and
write it back to the ORC file format using `orc()` method of
`DataFrameReader` and `DataFrameWriter`. In this lab, I will explain
how to read an ORC file into Spark DataFrame, proform some filtering,
creating a table by reading the ORC file, and finally writing is back by
partition using scala examples.



**Table of contents**

-   What is ORC?
-   ORC advantages
-   Write Spark DataFrame to ORC file
-   Read ORC file into Spark DataFrame
-   Creating a table on ORC file & using SQL
-   Using Partition
-   Which compression to choose

What is the ORC file?
-------------------------------------------------------------------------------------

[ORC](https://orc.apache.org/) stands of Optimized Row Columnar which
provides a highly efficient way to store the data in a self-describing,
type-aware column-oriented format for the Hadoop ecosystem. This is
similar to other columnar storage formats Hadoop supports such as
RCFile,
[parquet].



ORC file format heavily used as a storage for Apache Hive due to its
highly efficient way of storing data which enables high-speed processing
and ORC also used or natively supported by many frameworks like Hadoop
MapReduce, Apache Spark, Pig, Nifi, and many more.

### ORC Advantages

-   **Compression**: ORC stores data as columns and in compressed format
    hence it takes way less disk storage than other formats.
-   **Reduces I/O**: ORC reads only columns that are mentioned in a
    query for processing hence it takes reduces I/O.
-   **Fast reads**: ORC is used for high-speed processing as it by
    default creates built-in index and has some default aggregates like
    min/max values for numeric data.

ORC Compression
--------------------------------------------------------------------------

Spark supports the following compression options for ORC data source. By
default, it uses `SNAPPY` when not specified.

-   SNAPPY
-   ZLIB
-   LZO
-   NONE

Create a DataFrame
--------------------------------------------------------------------------------

Spark by default supports ORC file formats without importing third party
ORC dependencies. Since we don't have an ORC file to read, first will
create an ORC file from the DataFrame. Below is a sample DataFrame we
use to create an ORC file.



```
val data =Seq(("James ","","Smith","36636","M",3000),
  ("Michael ","Rose","","40288","M",4000),
  ("Robert ","","Williams","42114","M",4000),
  ("Maria ","Anne","Jones","39192","F",4000),
  ("Jen","Mary","Brown","","F",-1))
val columns=Seq("firstname","middlename","lastname","dob","gender","salary")
val df=spark.createDataFrame(data).toDF(columns:_*)
df.printSchema()
df.show(false)
```



Spark Write ORC file
------------------------------------------------------------------------------------

Spark `DataFrameWriter` uses `orc()` method to write or create ORC file
from DataFrame. This method takes a path as an argument where to write a
ORC file.

```
df.write.orc("/tmp/orc/data.orc")
```



Alternatively, you can also write using `format("orc")`

```
df.write.format("orc").save("/tmp/orc/data.orc")
```



![Spark write ORC in snappy
compression](./Lab_29_files/spark-read-orc-1.jpg)

Spark by default uses `snappy` compression while writing ORC file. You
can notice this on the part file names. And you can change the
compression from default `snappy` to either `none` or `zlib` using an
option compression

```
  df.write.mode("overwrite")
    .option("compression","zlib")
    .orc("/tmp/orc/data-zlib.orc")
```



This creates ORC files with `zlib` compression.

![Spark write ORC in zlib
compression]

Using append save mode, you can append a DataFrame to an existing ORC
file. Incase to overwrite use overwrite save mode.

```
df.write.mode('append').orc("/tmp/orc/people.orc")
df.write.mode('overwrite').orc("/tmp/orc/people.orc")
```



Spark Read ORC file
----------------------------------------------------------------------------------

Use Spark DataFrameReader's orc() method to read ORC file into
DataFrame. This supports reading snappy, zlib or no compression, it is
not necessary to specify in compression option while reading a ORC file.

```
df.read.orc("/tmp/orc/data.orc")
```



In order to read ORC files from Amazon S3, use the below prefix to the
path along with [third-party dependencies and
credentials].

-   **s3:\\\\** = \> First gen
-   **s3n:\\\\** =\> second Gen
-   **s3a:\\\\** =\> Third gen

Executing SQL queries on DataFrame
----------------------------------------------------------------------------------------------------------------

We can also create a temporary view on Stark DataFrame that was created
on ORC file and run SQL queries.. These views are available until your
program exits.

```
df2.createOrReplaceTempView("ORCTable")
val orcSQL = spark.sql("select firstname,dob from ORCTable where salary >= 4000 ")
orcSQL.show(false)
```



In this example, the physical table scan loads only
columns **firstname**, **dob,** and age at runtime, without reading
**all** columns from the file system. This improves read performance.

Creating a table on ORC file
----------------------------------------------------------------------------------------------------

Now let's walk through executing SQL queries on the ORC file without
creating a DataFrame first. In order to execute SQL queries, create a
temporary view or table directly on the ORC file instead of creating
from DataFrame.

```
spark.sql("CREATE TEMPORARY VIEW PERSON USING orc OPTIONS (path \"/tmp/orc/data.orc\")")
spark.sql("SELECT * FROM PERSON").show()
```



Here, we created a temporary view `PERSON` from ORC file "`data`" file.
This gives the following results.

```
+---------+----------+--------+-----+------+------+
|firstname|middlename|lastname|  dob|gender|salary|
+---------+----------+--------+-----+------+------+
|  Robert |          |Williams|42114|     M|  4000|
|   Maria |      Anne|   Jones|39192|     F|  4000|
| Michael |      Rose|        |40288|     M|  4000|
|   James |          |   Smith|36636|     M|  3000|
|      Jen|      Mary|   Brown|     |     F|    -1|
+---------+----------+--------+-----+------+------+
```



Using Partition
--------------------------------------------------------------------------

When we execute a particular query on PERSON table, it scan's through
all the rows and returns the results the selected columns back. In
Spark, we can improve query execution in an optimized way by doing
partitions on the data using `partitionBy()` method. Following is the
example of partitionBy().

```
df.write.partitionBy("gender","salary")
    .mode("overwrite").orc("/tmp/orc/data.orc")
```



When you check the people.orc file, it has two partitions "gender"
followed by "salary" inside.

### Reading a specific Partition

The example below explains of reading partitioned ORC file into
DataFrame with gender=M.

```
val parDF=spark.read.orc("/tmp/orc/data.orc/gender=M")
parDF.show(false)
```



Which compression to choose
--------------------------------------------------------------------------------------------------

Not writing ORC files in compression results in larger disk space and
slower in performance. Hence, it is suggestable to use compression.
Below are basic comparison between ZLIB and SNAPPY when to use what.

-   When you need a faster read then ZLIB compression is to-go option,
    without a doubt, It also takes smaller storage on disk compared with
    SNAPPY.
-   ZLIB is slightly slower in write compared with SNAPPY. If you have
    large data set to write, use SNAPPY. For smaller datasets, it is
    still suggestible to use ZLIB.

Complete Example of using ORC in Spark
------------------------------------------------------------------------------------------------------------------------

```
import org.apache.spark.sql.{SparkSession}

object ReadORCFile extends App{

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("sparkexamples")
    .getOrCreate()

  val data =Seq(("James ","","Smith","36636","M",3000),
  ("Michael ","Rose","","40288","M",4000),
  ("Robert ","","Williams","42114","M",4000),
  ("Maria ","Anne","Jones","39192","F",4000),
  ("Jen","Mary","Brown","","F",-1))
  val columns=Seq("firstname","middlename","lastname","dob","gender","salary")
  val df=spark.createDataFrame(data).toDF(columns:_*)

  df.write.mode("overwrite")
    .orc("/tmp/orc/data.orc")

  df.write.mode("overwrite")
    .option("compression","none12")
    .orc("/tmp/orc/data-nocomp.orc")

  df.write.mode("overwrite")
    .option("compression","zlib")
    .orc("/tmp/orc/data-zlib.orc")

  val df2=spark.read.orc("/tmp/orc/data.orc")
  df2.show(false)

  df2.createOrReplaceTempView("ORCTable")
  val orcSQL = spark.sql("select firstname,dob from ORCTable where salary >= 4000 ")
  orcSQL.show(false)

  spark.sql("CREATE TEMPORARY VIEW PERSON USING orc OPTIONS (path \"/tmp/orc/data.orc\")")
  spark.sql("SELECT * FROM PERSON").show()
}
```



### Conclusion

In summary, ORC is a high efficient, compressed columnar format that is
capable to store petabytes of data without compromising fast reads.
Spark natively supports ORC data source to read and write an ORC files
using orc() method on DataFrameReader and DataFrameWrite.

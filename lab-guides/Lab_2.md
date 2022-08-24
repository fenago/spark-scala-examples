

Spark -- What is SparkSession Explained
=======================================



Since Spark 2.0, SparkSession has become an entry point to Spark to work
with RDD, DataFrame, and Dataset. Prior to 2.0, SparkContext used to be
an entry point. Here, I will mainly focus on explaining what is
SparkSession by defining and describing how to create Spark Session and
using the default Spark Session 'spark' variable from spark-shell.





**What is SparkSession**

SparkSession was introduced in version Spark 2.0, It is an entry point
to underlying Spark functionality in order to programmatically create
Spark RDD, DataFrame, and DataSet. SparkSession's object *`spark`* is
the default variable available in `spark-shell` and it can be created
programmatically using `SparkSession` builder pattern.



1. SparkSession in Spark 2.0
--------------------------------------------------------------------------------------------------

-   With Spark 2.0 a new class `org.apache.spark.sql.SparkSession` has
    been introduced which is a combined class for all different contexts
    we used to have prior to 2.0 (`SQLContext` and `HiveContext` e.t.c)
    release hence, Spark Session can be used in the place of SQLContext,
    HiveContext, and other contexts.

-   As mentioned in the beginning SparkSession is an entry point to
    Spark and creating a SparkSession instance would be the first
    statement you would write to program with
    [RDD],
    [DataFrame],
    and Dataset. SparkSession will be created using
    `SparkSession.builder()` builder patterns.

-   Prior to Spark 2.0, SparkContext used to be an entry point, and it's
    not been completely replaced with SparkSession, many features of
    SparkContext are still available and used in Spark 2.0 and later.
    You should also know that SparkSession internally creates
    `SparkConfig` and `SparkContext` with the configuration provided
    with SparkSession.

-   Spark Session also includes all the APIs available in different
    contexts --
    -   SparkContext
    -   SQLContext
    -   StreamingContext
    -   HiveContext

**How many SparkSessions can you create in an application?**



You can create as many `SparkSession` as you want in a Spark application
using
either `SparkSession.builder()` or `SparkSession.newSession()`. Many
Spark session objects are required when you wanted to keep Spark tables
(relational entities) logically separated.

2. SparkSession in spark-shell
-------------------------------------------------------------------------------------------------------

Be default Spark shell provides `spark` object which is an instance of
SparkSession class. We can directly use this object when required in
[spark-shell].

```
// Usage of spark variable
scala> spark.version
```



Similar to the Spark shell, In most of the tools, notebooks, and Azure
Databricks, the environment itself creates a default SparkSession object
for us to use so you don't have to worry about creating a spark session.



### 3. Create SparkSession From Scala Program

To create SparkSession in Scala or Python, you need to use the builder
pattern method `builder()` and calling `getOrCreate()` method. If
SparkSession already exists it returns otherwise creates a new
SparkSession.

```
// Create SparkSession object
import org.apache.spark.sql.SparkSession
object SparkSessionTest extends App {
  val spark = SparkSession.builder()
      .master("local[1]")
      .appName("sparkexamples")
      .getOrCreate();
  println(spark)
  println("Spark Version : "+spark.version)
}

// Outputs
//org.apache.spark.sql.SparkSession@2fdf17dc
//Spark Version : 3.2.1
```



From the above code --

`SparkSession.builder()` -- Return `SparkSession.Builder` class. This is
a builder for `SparkSession`. master(), appName() and getOrCreate() are
methods of
[SparkSession.Builder](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/SparkSession.Builder.html).






`master()` -- If you are running it on the cluster you need to use your
master name as an argument to master(). usually, it would be
either [`yarn` ]or `mesos` depends
on your cluster setup.

-   Use `local[x]` when running in Standalone mode. x should be an
    integer value and should be greater than 0; this represents how many
    partitions it should create when using RDD, DataFrame, and Dataset.
    Ideally, x value should be the number of CPU cores you have.
-   For standalone use `spark://master:7077`

`appName()` -- Sets a name to the Spark application that shows in the
[Spark web
UI]. If
no application name is set, it sets a random name.

`getOrCreate()` -- This returns a SparkSession object if already exists.
Creates a new one if not exist.

### 3.1 Get Existing SparkSession

You can get the existing SparkSession in Scala programmatically using
the below example.

```
// Get existing SparkSession 
import org.apache.spark.sql.SparkSession
val spark2 = SparkSession.builder().getOrCreate()
print(spark2)

// Outputs
// org.apache.spark.sql.SparkSession@2fdf17dc
```



Compare the hash of spark and spark2 object. Since it returned the
existing session, both objects have the same hash value.

### 3.2 Create Another SparkSession

Sometimes you might be required to create multiple sessions, which you
can easily achieve by using `newSession()` method. This uses the same
app name, master as the existing session. Underlying SparkContext will
be the same for both sessions as you can have only one context per Spark
application.

```
// Create a new SparkSession
val spark3 = spark.newSession()
print(spark3)

// Outputs
// org.apache.spark.sql.SparkSession@692dba54
```



Compare this hash with the hash from the above example, it should be
different.

### 3.3 Setting Spark Configs

If you wanted to set some configs to SparkSession, use the `config()`
method.

```
// Usage of config()
val spark = SparkSession.builder()
      .master("local[1]")
      .appName("sparkexamples")
      .config("spark.some.config.option", "config-value")
      .getOrCreate();
```



### 3.4 Create SparkSession with Hive Enable

In order to use Hive with Spark, you need to enable it using the
`enableHiveSupport()` method.

```
// Enabling Hive to use in Spark
val spark = SparkSession.builder()
      .master("local[1]")
      .appName("sparkexamples")
      .config("spark.sql.warehouse.dir", "<path>/spark-warehouse")
      .enableHiveSupport()
      .getOrCreate();
```



4. Other Usages of SparkSession
---------------------------------------------------------------------------------------------------------

### 4.1 Set & Get All Spark Configs

Once the SparkSession is created, you can add the spark configs during
runtime or get all configs.

```
// Set Config
spark.conf.set("spark.sql.shuffle.partitions", "30")

// Get all Spark Configs
val configMap:Map[String, String] = spark.conf.getAll
```



### 4.2 Create DataFrame

SparkSession also provides several methods to [create a Spark
DataFrame]
and DataSet. The below example uses the `createDataFrame()` method which
takes a list of data.

```
// Create DataFrame
val df = spark.createDataFrame(
    List(("Scala", 25000), ("Spark", 35000), ("PHP", 21000)))
df.show()

// Output
//+-----+-----+
//|   _1|   _2|
//+-----+-----+
//|Scala|25000|
//|Spark|35000|
//|  PHP|21000|
//+-----+-----+
```



### 4.3 Working with Spark SQL

Using SparkSession you can access Spark SQL capabilities in Apache
Spark. In order to use SQL features first, you need to create a
temporary view in Spark. Once you have a temporary view you can run any
ANSI SQL queries using `spark.sql()` method.

```
// Spark SQL
df.createOrReplaceTempView("sample_table")
val df2 = spark.sql("SELECT _1,_2 FROM sample_table")
df2.show()
```



Spark SQL temporary views are session-scoped and will not be available
if the session that creates it terminates. If you want to have a
temporary view that is shared among all sessions and keep alive until
the Spark application terminates, you can create a global temporary view
using `createGlobalTempView()`

### 4.4 Create Hive Table

As explained above SparkSession is used to create and query Hive tables.
Note that in order to do this for testing you don't need Hive to be
installed. `saveAsTable()` creates Hive managed table. Query the table
using `spark.sql()`.

```
// Create Hive table & query it.  
spark.table("sample_table").write.saveAsTable("sample_hive_table")
val df3 = spark.sql("SELECT _1,_2 FROM sample_hive_table")
df3.show()
```



### 4.5 Working with Catalogs

To get the catalog metadata, Spark Session exposes `catalog` variable.
Note that these methods `spark.catalog.listDatabases` and
`spark.catalog.listTables` and returns the DataSet.

```
// Get metadata from the Catalog
// List databases
val ds = spark.catalog.listDatabases
ds.show(false)

// Output
//+-------+----------------+----------------------------+
//|name   |description     |locationUri                 |
//+-------+----------------+----------------------------+
//|default|default database|file:/<path>/spark-warehouse|
//+-------+----------------+----------------------------+

// List Tables
val ds2 = spark.catalog.listTables
ds2.show(false)

//Output
//+-----------------+--------+-----------+---------+-----------+
//|name             |database|description|tableType|isTemporary|
//+-----------------+--------+-----------+---------+-----------+
//|sample_hive_table|default |null       |MANAGED  |false      |
//|sample_table     |null    |null       |TEMPORARY|true       |
//+-----------------+--------+-----------+---------+-----------+
```



Notice the two tables we have created, Spark table is considered a
temporary table and [Hive table as managed
table].

5. SparkSession Commonly Used Methods
---------------------------------------------------------------------------------------------------------------------

`version` -- Returns [Spark
version] where
your application is running, probably the Spark version your cluster is
configured with.

`catalog` -- Returns the catalog object to access metadata.

`conf` -- Returns the RuntimeConfig object.

`builder()` -- builder() is used to create a new SparkSession, this
return `SparkSession.Builder`

`newSession()` -- Creaetes a new SparkSession.

`range(n)` -- Returns a single
column [`Dataset`](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/Dataset.html) with
`LongType` and column named `id`, containing elements in a range from 0
to `n` (exclusive) with step value 1. There are several variations of
this function, refer to Spark documentation.

`createDataFrame()` -- This [creates a
DataFrame]
from a collection and an
[RDD]

`createDataset()` -- This [creates a
Dataset]
from the collection, DataFrame, and RDD.

`emptyDataFrame()` -- Creates an [empty
DataFrame].

`emptyDataset()` -- Creates an [empty
Dataset].

`getActiveSession()` -- Returns an active Spark session for the current
thread.

`getDefaultSession()` -- Returns the default SparkSession that is
returned by the builder.

`implicits()` -- To access the nested Scala object.

`read()` -- Returns an instance of `DataFrameReader` class, this is used
to read records from CSV, Parquet, Avro, and more file formats into
DataFrame.

`readStream()` -- Returns an instance of `DataStreamReader` class, this
is used to read streaming data. that can be used to read streaming data
into DataFrame.

`sparkContext()` -- Returns a
[SparkContext].

`sql(String sql)` -- Returns a DataFrame after executing the SQL
mentioned.

`sqlContext()` -- Returns
[SQLContext].

`stop()` -- Stop the current
[SparkContext].

`table()` -- Returns a DataFrame of a table or view.

`udf()` -- Creates a [Spark UDF to use it on
DataFrame], Dataset,
and SQL.

6. Conclusion
-------------

In this Spark SparkSession lab, you have learned what is Spark
Session, its usage, how to create SparkSession programmatically, and
finally learned some of the commonly used SparkSession methods.


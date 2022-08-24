
Spark 3.0 Read Binary File into DataFrame
=========================================


Since Spark 3.0, Spark supports a data source format `binaryFile` to
read binary file (image, pdf, zip, gzip, tar e.t.c) into Spark
DataFrame/Dataset. When used `binaryFile` format,
the DataFrameReader converts the entire contents of each binary file
into a single DataFrame, the resultant DataFrame contains the raw
content and metadata of the file. 



In this Spark 3.0 article, I will provide a Scala example of how to read
single, multiple, and all binary files from a folder into DataFrame and
also know different options it supports.

Using `binaryFile` data source, you should able to read files like
image, pdf, zip, gzip, tar, and many binary files into DataFrame, each
file will be read as a single record along with the metadata of the
file. The resultant DataFrame contains the following columns.



-   `path`: StringType **=\>** Absolute path of the file
-   `modificationTime`: TimestampType **=\>** Last modified time stamp
    of the file
-   `length`: LongType **=\>** length of the file
-   `content`: BinaryType **=\>** binary contents of the file

Read a Single Binary File
----------------------------------------------------------------------------------------------

The below example read the spark.png image binary file into DataFrame.
The RAW data of the file will be loaded into `content` column.

```
  val df = spark.read.format("binaryFile").load("/tmp/binary/spark.png")
  df.printSchema()
  df.show()
```



This returns the below schema and DataFrame.

```
root
 |-- path: string (nullable = true)
 |-- modificationTime: timestamp (nullable = true)
 |-- length: long (nullable = true)
 |-- content: binary (nullable = true)

+--------------------+--------------------+------+--------------------+
|                path|    modificationTime|length|             content|
+--------------------+--------------------+------+--------------------+
|file:/C:/tmp/bina...|2020-07-25 10:11:...| 74675|[89 50 4E 47 0D 0...|
+--------------------+--------------------+------+--------------------+
```



The data in `content` column shows binary data.



In order to read binary files from Amazon S3 using the below prefix to
the path along with [third-party dependencies and
credentials].

-   **s3:\\\\** = \> First gen
-   **s3n:\\\\** =\> second Gen
-   **s3a:\\\\** =\> Third gen

Read Multiple Binary Files
------------------------------------------------------------------------------------------------

The below example reads all PNG image files from a path into Spark
DataFrame.

```
  val df3 = spark.read.format("binaryFile").load("/tmp/binary/*.png")
  df3.printSchema()
  df3.show(false)
```



It reads all `png` files and converts each file into a single record in
DataFrame.






Read all Binary Files in a Folder
--------------------------------------------------------------------------------------------------------------

In order to read all binary files from a folder, just pass the folder
path to the `load()` method.

```
  val df2 = spark.read.format("binaryFile").load("/tmp/binary/")
  df2.printSchema()
  df2.show(false)
```



Reading Binary File Options
--------------------------------------------------------------------------------------------------

`pathGlobFilter`: To load files with paths matching a given glob pattern
while keeping the behavior of partition discovery.

For example, the following code reads all PNG files from the path with
any partitioned directories.

```
  val df = spark.read.format("binaryFile")
              .option("pathGlobFilter", "*.png")
              .load("/tmp/binary/")
```



`recursiveFileLookup`: Ignores the partition discovery and recursively
search files under the input directory path.

```
  val df = spark.read.format("binaryFile")
            .option("pathGlobFilter", "*.png")
            .option("recursiveFileLookup", "true")
            .load("/tmp/binary/")
```



Few things to note
--------------------------------------------------------------------------------

-   While using `binaryFile` data source, if you pass text file to the
    `load()` method, it reads the contents of the text file as a binary
    into DataFrame.

-   binary() method on `DataFrameReader` still not available hence, you
    can't use `spark.read.binary("path")` yet. I will update this
    article when it's available.

-   Currently, the binary file data source does not support writing a
    DataFrame back to the binary file format.

Conclusion
----------

In summary, Spark 3.0 provides a `binaryFile` data source to read the
binary file into DataFrame but it does not support writing the data
frame back into a binary file. It also has option pathGlobFilter to load
files by preserving the partition and `recursiveFileLookup` option to
recursively load the files from the subdirectories by ignoring
partition.

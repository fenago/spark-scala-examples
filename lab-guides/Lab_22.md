

Spark Read XML file using Databricks API
========================================



Apache Spark can also be used to process or read simple to complex
nested XML files into Spark DataFrame and writing it back to XML using
Databricks [Spark XML API](https://github.com/databricks/spark-xml)
(spark-xml) library. In this lab, I will explain how to read XML
file with several options using the Scala example.


Databricks Spark-XML Maven dependency
----------------------------------------------------------------------------------------------------------------------

Processing XML files in Apache Spark is enabled by using below
Databricks spark-xml dependency into the maven pom.xml file.

```
<dependency>
     <groupId>com.databricks</groupId>
     <artifactId>spark-xml_2.11</artifactId>
     <version>0.6.0</version>
 </dependency>
```

Spark Read XML into DataFrame
------------------------------------------------------------------------------------------------------

Databricks Spark-XML package allows us to read simple or nested XML
files into DataFrame, once DataFrame is created, we can leverage its
APIs to perform transformations and actions like any other DataFrame.



Spark-XML API accepts several options while reading an XML file. for
example, option `rowTag `is used to specify the rows tag. `rootTag` is
used to specify the root tag of the input nested XML

Input XML file we use on this example is available at
[GitHub](https://github.com/sparkbyexamples/spark-examples/blob/master/spark-sql-examples/src/main/resources/persons.xml)
repository.

```
val df = spark.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "person")
      .xml("src/main/resources/persons.xml")
```



Alternatively, you can also use the short form `format("xml")` and
`load("src/main/resources/persons.xml")`



While API reads XML file into DataFrame, It automatically infers the
schema based on data. Below schema ouputs from `df.printSchma()` .


```
root
 |-- _id: long (nullable = true)
 |-- dob_month: long (nullable = true)
 |-- dob_year: long (nullable = true)
 |-- firstname: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- lastname: string (nullable = true)
 |-- middlename: string (nullable = true)
 |-- salary: struct (nullable = true)
 |    |-- _VALUE: long (nullable = true)
 |    |-- _currency: string (nullable = true)
```



We can also supply our own struct schema and use it while reading a file
as described below.

```
 val schema = new StructType()
      .add("_id",StringType)
      .add("firstname",StringType)
      .add("middlename",StringType)
      .add("lastname",StringType)
      .add("dob_year",StringType)
      .add("dob_month",StringType)
      .add("gender",StringType)
      .add("salary",StringType)
val df = spark.read
  .option("rowTag", "book")
  .schema(schema)
  .xml("src/main/resources/persons.xml")
df.show()
```



Output:






`show()` on DataFrame outputs the following.


```
+---+---------+--------+---------+------+--------+----------+---------------+
|_id|dob_month|dob_year|firstname|gender|lastname|middlename|         salary|
+---+---------+--------+---------+------+--------+----------+---------------+
|  1|        1|    1980|    James|     M|   Smith|      null|  [10000, Euro]|
|  2|        6|    1990|  Michael|     M|    null|      Rose|[10000, Dollor]|
+---+---------+--------+---------+------+--------+----------+---------------+
```



#### Handling XML Attributes

"\_" is added to the variable prefix for attributes, for example,
\_value & \_currency are attributes from XML file. We can change the
prefix to be any special character by using the option `attributePrefix`
. Handling attributes can be disabled with the option `excludeAttribute`

Spark Write DataFrame to XML File
--------------------------------------------------------------------------------------------------------------

Use "com.databricks.spark.xml" DataSource on format method of the
DataFrameWriter to write Spark DataFrame to XML file. This data source
is provided as part of the Spark-XML API. simar to reading, write also
takes options rootTag and rowTag to specify the root tag and row tag
respectively on the output XML file.

```
df2.write
      .format("com.databricks.spark.xml")
      .option("rootTag", "persons")
      .option("rowTag", "person")
      .save("src/main/resources/persons_new.xml")
```



This snippet writes a Spark DataFrame "df2" to XML file
"pesons\_new.xml" with "persons" as root tag and "person" as row tag.

### Limitations:

This API is most useful when reading and writing simple XML files.
However, At the time of writing this lab, this API has the following
limitations.

-   Reading/Writing attribute to/from root element not supported in this
    API.
-   Doesn't support complex XML structures where you want to read header
    and footer along with row elements.

If you have one root element following data elements then Spark XML is
GO to API. If you wanted to write a complex structure and this API is
not suitable for you, please read below lab where I've explained
using XStream API

[Spark -- Writing complex XML structures using XStream
API]

Write Spark XML DataFrame to Avro File
------------------------------------------------------------------------------------------------------------------------

Once you create a DataFrame by reading XML, We can easily write it to
Avro by using below maven dependency.

Apache Avro is a serialization system and is used to store persistent
data in a binary format. When Avro data is stored in a file, its schema
is stored with it, so that files may be processed later by any program.

```
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-avro_2.11</artifactId>
    <version>2.4.0</version>
</dependency>
```

`format("avro")` is provided by spark-avro API to read/write Avro files.

```
df2.write.format("avro")
      .mode(SaveMode.Overwrite)
      .save("\tmp\spark_out\avro\persons.avro")
```



Below snippet provides writing to Avro file by using partitions.

```
df2.write.partitionBy("_id")
        .format("avro").save("persons_partition.avro")
```



Write Spark XML DataFrame to Parquet File
------------------------------------------------------------------------------------------------------------------------------

Spark SQL provides a `parquet` method to read/write parquet files hence,
no additional libraries are not needed, once the DatraFrame created from
XML we can use the parquet method on DataFrameWriter class to write to
the Parquet file.

Apache Parquet is a columnar file format that provides optimizations to
speed up queries and is a far more efficient file format than CSV or
JSON. Spark SQL comes with a `parquet ` method to read data. It
automatically captures the schema of the original data and reduces data
storage by 75% on average.

```
df2.write
      .parquet("\tmp\spark_output\parquet\persons.parquet")
```



Below snippet, writes DataFrame to parquet file with partition by
"\_id".

```
df2.write
      .partitionBy("_id")
      .parquet("\tmp\spark_output\parquet\persons_partition.parquet")
```



#### Conclusion:

In this lab, you have learned how to read XML files into Apache
Spark DataFrame and write it back to XML, Avro, and Parquet files after
processing using spark xml API. Also, explains some limitations of using
Databricks Spark-XML API.

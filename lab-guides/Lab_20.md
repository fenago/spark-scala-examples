
Spark Read and Write JSON file into DataFrame
=============================================




1. Spark Read JSON File into DataFrame
-----------------------------------------------------------------------------------------------------------------------

Using `spark.read.json("path")` or
`spark.read.format("json").load("path")` you can read a JSON file into a
Spark DataFrame, these methods take a file path as an argument. 

Unlike [reading a
CSV],
By default JSON data source inferschema from an input file.



Refer dataset used in this article at [zipcodes.json on
GitHub](https://github.com/fenago/spark-scala-examples/blob/master/src/main/resources/zipcodes.json)

```
//read json file into dataframe
val df = spark.read.json("src/main/resources/zipcodes.json")
df.printSchema()
df.show(false)
```



When you use` format("json")` method, you can also specify the Data
sources by their fully qualified name (i.e.,
`org.apache.spark.sql.json`), for built-in sources, you can also use
short name "json". 

2. Read JSON file from multiline
-----------------------------------------------------------------------------------------------------------

Sometimes you may want to read records from JSON file that scattered
multiple lines, In order to read such files, use-value true to
[multiline]
option, by default multiline option, is set to false.






Below is the input file we going to read, this same file is also
available at [multiline-zipcode.json on
GitHub](https://github.com/fenago/spark-scala-examples/blob/master/src/main/resources/multiline-zipcode.json). 

```
[{
  "RecordNumber": 2,
  "Zipcode": 704,
  "ZipCodeType": "STANDARD",
  "City": "PASEO COSTA DEL SUR",
  "State": "PR"
},
{
  "RecordNumber": 10,
  "Zipcode": 709,
  "ZipCodeType": "STANDARD",
  "City": "BDA SAN LUIS",
  "State": "PR"
}]
```



Using `spark.read.option("multiline","true")`

```
//read multiline json file
val multiline_df = spark.read.option("multiline","true")
      .json("src/main/resources/multiline-zipcode.json")
multiline_df.show(false)    
```



3. Reading Multiple Files at a Time
-----------------------------------------------------------------------------------------------------------------

Using the `spark.read.json()` method you can also read multiple JSON
files from different paths, just pass all file names with fully
qualified paths by separating comma, for example

```
//read multiple files
val df2 = spark.read.json(
     "src/main/resources/zipcodes_streaming/zipcode1.json",
     "src/main/resources/zipcodes_streaming/zipcode2.json")
df2.show(false)
```



4. Reading all Files in a Directory
-----------------------------------------------------------------------------------------------------------------

We can read all JSON files from a directory into DataFrame just by
passing directory as a path to the `json()` method. Below snippet,
"[zipcodes\_streaming](https://github.com/fenago/spark-scala-examples/tree/master/src/main/resources/zipcodes_streaming)"
is a folder that contains multiple JSON files.

```
//read all files from a folder
val df3 = spark.read.json("src/main/resources/zipcodes_streaming")
df3.show(false)
```



5. Reading files with a user-specified custom schema
---------------------------------------------------------------------------------------------------------------------------------------------------

Spark Schema defines the structure of the data, in other words, it is
the structure of the DataFrame. Spark SQL provides StructType &
StructField classes to programmatically specify the structure to the
DataFrame.

If you know the
[schema]
of the file ahead and do not want to use the default `inferSchema`
option for column names and types, use user-defined custom column names
and type using schema option.

Use the [StructType class to create a custom
schema],
below we initiate this class and use add a method to add columns to it
by providing the column name, data type and nullable option.

```
//Define custom schema
val schema = new StructType()
      .add("RecordNumber",IntegerType,true)
      .add("Zipcode",IntegerType,true)
      .add("ZipCodeType",StringType,true)
      .add("City",StringType,true)
      .add("State",StringType,true)
      .add("LocationType",StringType,true)
      .add("Lat",DoubleType,true)
      .add("Long",DoubleType,true)
      .add("Xaxis",IntegerType,true)
      .add("Yaxis",DoubleType,true)
      .add("Zaxis",DoubleType,true)
      .add("WorldRegion",StringType,true)
      .add("Country",StringType,true)
      .add("LocationText",StringType,true)
      .add("Location",StringType,true)
      .add("Decommisioned",BooleanType,true)
      .add("TaxReturnsFiled",StringType,true)
      .add("EstimatedPopulation",IntegerType,true)
      .add("TotalWages",IntegerType,true)
      .add("Notes",StringType,true)
val df_with_schema = spark.read.schema(schema)
        .json("src/main/resources/zipcodes.json")
df_with_schema.printSchema()
df_with_schema.show(false)
```



6. Read JSON file using Spark SQL
-------------------------------------------------------------------------------------------------------------

Spark SQL also provides a way to read a JSON file by creating a
temporary view directly from reading file using
spark.sqlContext.sql("load json to temporary view")

```
spark.sqlContext.sql("CREATE TEMPORARY VIEW zipcode USING json OPTIONS" + 
      " (path 'src/main/resources/zipcodes.json')")
spark.sqlContext.sql("select * from zipcodes").show(false)
```



7. Options while reading JSON file
---------------------------------------------------------------------------------------------------------------

### 7.1 nullValues

Using `nullValues` option you can specify the string in a JSON to
consider as null. For example, if you want to consider a date column
with a value "1900-01-01" set null on DataFrame.

### 7.2 dateFormat

`dateFormat` option to used to set the format of the input [DateType and
TimestampType]
columns. Supports all
[java.text.SimpleDateFormat](https://docs.oracle.com/javase/10/docs/api/java/time/format/DateTimeFormatter.html)
formats.

**Note:** Besides the above options, Spark JSON dataset also supports
many other options.

8. Applying DataFrame Transformations
---------------------------------------------------------------------------------------------------------------------

Once you have [created
DataFrame]
from the JSON file, you can apply all transformation and actions
DataFrame support. Please refer to the link for more details. 

9. Write Spark DataFrame to JSON file
---------------------------------------------------------------------------------------------------------------------

Use the Spark DataFrameWriter object "write" method on DataFrame to
write a JSON file. 

```
df2.write
 .json("/tmp/spark_output/zipcodes.json")
```



### 9.1 Spark Options while writing JSON files

While writing a JSON file you can use several options.  

Other options available `nullValue`,`dateFormat`

### 9.2 Saving modes

Spark DataFrameWriter also has a method mode() to specify SaveMode; the
argument to this method either takes below string or a constant from
`SaveMode` class.

overwrite -- mode is used to overwrite the existing file, alternatively,
you can use `SaveMode.Overwrite`.

append -- To add the data to the existing file, alternatively, you can
use `SaveMode.Append`.

ignore -- Ignores write operation when the file already exists,
alternatively you can use `SaveMode.Ignore`.

errorifexists or error -- This is a default option when the file already
exists, it returns an error, alternatively, you can use
`SaveMode.ErrorIfExists`.

```
df2.write.mode(SaveMode.Overwrite).json("/tmp/spark_output/zipcodes.json")
```



10. Source Code for Reference
-----------------------------------------------------------------------------------------------------

```
package com.sparkbyexamples.spark.dataframe
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
object FromJsonFile {
  def main(args:Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("SparkByExample")
      .getOrCreate()
    val sc = spark.sparkContext
    //read json file into dataframe
    val df = spark.read.json("src/main/resources/zipcodes.json")
    df.printSchema()
    df.show(false)
    //read multiline json file
    val multiline_df = spark.read.option("multiline", "true")
      .json("src/main/resources/multiline-zipcode.json")
    multiline_df.printSchema()
    multiline_df.show(false)
    //read multiple files
    val df2 = spark.read.json(
      "src/main/resources/zipcodes_streaming/zipcode1.json",
      "src/main/resources/zipcodes_streaming/zipcode2.json")
    df2.show(false)
    //read all files from a folder
    val df3 = spark.read.json("src/main/resources/zipcodes_streaming/*")
    df3.show(false)
    //Define custom schema
    val schema = new StructType()
      .add("City", StringType, true)
      .add("Country", StringType, true)
      .add("Decommisioned", BooleanType, true)
      .add("EstimatedPopulation", LongType, true)
      .add("Lat", DoubleType, true)
      .add("Location", StringType, true)
      .add("LocationText", StringType, true)
      .add("LocationType", StringType, true)
      .add("Long", DoubleType, true)
      .add("Notes", StringType, true)
      .add("RecordNumber", LongType, true)
      .add("State", StringType, true)
      .add("TaxReturnsFiled", LongType, true)
      .add("TotalWages", LongType, true)
      .add("WorldRegion", StringType, true)
      .add("Xaxis", DoubleType, true)
      .add("Yaxis", DoubleType, true)
      .add("Zaxis", DoubleType, true)
      .add("Zipcode", StringType, true)
      .add("ZipCodeType", StringType, true)
    val df_with_schema = spark.read.schema(schema)
        .json("src/main/resources/zipcodes.json")
    df_with_schema.printSchema()
    df_with_schema.show(false)
    spark.sqlContext.sql("CREATE TEMPORARY VIEW zipcode USING json OPTIONS" +
      " (path 'src/main/resources/zipcodes.json')")
    spark.sqlContext.sql("SELECT *FROM zipcode").show()
    //Write json file
    df2.write
      .json("/tmp/spark_output/zipcodes.json")
  }
}
```



Conclusion:
-----------

In this tutorial, you have learned how to read a JSON file with single
line record and multiline record into Spark DataFrame, and also learned
reading single and multiple files at a time and writing JSON file back
to DataFrame using different save options.


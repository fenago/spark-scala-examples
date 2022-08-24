

Spark Read CSV file into DataFrame
==================================



Spark SQL provides `spark.read.csv("path")` to read a CSV file into
Spark DataFrame and `dataframe.write.csv("path")` to save or write to
the CSV file. Spark supports reading pipe, comma, tab, or any other
delimiter/seperator files.



In this tutorial, you will learn how to read a single file, multiple
files, all files from a local directory into DataFrame, and applying
some transformations finally writing DataFrame back to CSV file using
Scala.

**Note:** Spark out of the box supports to read files in
[CSV],
[JSON],
[TEXT],
Parquet, and many more file formats into Spark DataFrame. 



Spark Read CSV file into DataFrame
----------------------------------------------------------------------------------------------------------------

Using `spark.read.csv("path")` or
`spark.read.format("csv").load("path")` you can read a CSV file with
fields delimited by pipe, comma, tab (and many more) into a Spark
DataFrame, These methods take a file path to read from as an argument.
You can find the [zipcodes.csv at
GitHub](https://github.com/fenago/spark-scala-examples/blob/3ea16e4c6c1614609c2bd7ebdffcee01c0fe6017/src/main/resources/zipcodes.csv)

```
 
val df = spark.read.csv("src/main/resources/zipcodes.csv")
    df.printSchema()
```



This example reads the data into DataFrame columns "\_c0" for the first
column and "\_c1" for second and so on. and by default type of all these
columns would be String.



```
 
root
 |-- _c0: string (nullable = true)
 |-- _c1: string (nullable = true)
 |-- _c2: string (nullable = true)
```



If you have a header with column names on file, you need to explicitly
specify `true` for header option using `option("header",true)` not
mentioning this, the API treats the header as a data record.

```
 
val df = spark.read.option("header",true)
   .csv("src/main/resources/zipcodes.csv")
```



It also reads all columns as a string
([StringType])
by default. I will explain in later sections how to read the
[schema]
(`inferschema`) from the header record and derive the [column
type]
based on the data.

When you use `format("csv")` method, you can also specify the Data
sources by their fully qualified name
(i.e., `org.apache.spark.sql.csv`), but for built-in sources, you can
also use their short names (`csv`,`json`, `parquet`, `jdbc`, `text`
e.t.c). 






### Read multiple CSV files

Using the `spark.read.csv()` method you can also read multiple CSV
files, just pass all file names by separating comma as a path, for
example : 

```
 
val df = spark.read.csv("path1,path2,path3")
```



#### Read all CSV files in a directory

 We can read all CSV files from a directory into DataFrame just by
passing the directory as a path to the `csv()` method.

```
 
val df = spark.read.csv("Folder path")
```



Options while reading CSV file
--------------------------------------------------------------------------------------------------------

Spark CSV dataset provides multiple options to work with CSV files.
Below are some of the most important options explained with examples.

### delimiter

`delimiter` option is used to specify the column delimiter of the CSV
file. By default, it is comma (,) character, but can be set to pipe
(\|), tab, space, or any character using this option.

```
 
val df2 = spark.read.options(Map("delimiter"->","))
  .csv("src/main/resources/zipcodes.csv")
```



### inferSchema

The default value set to this option is `false` when setting to `true`
it automatically infers column types based on the data. Note that, it
requires reading the data one more time to infer the
[schema].

```
 
val df2 = spark.read.options(Map("inferSchema"->"true","delimiter"->","))
  .csv("src/main/resources/zipcodes.csv")
```



### header

This option is used to read the first line of the CSV file as column
names. By default the value of this option is `false` , and all column
types are assumed to be a string.

```
 
val df2 = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
  .csv("src/main/resources/zipcodes.csv")
```



### quotes

When you have a column with a delimiter that used to split the columns,
use `quotes` option to specify the quote character, by default it is "
and delimiters inside quotes are ignored. but using this option you can
set any character.

### nullValues

Using `nullValues` option you can specify the string in a CSV to
consider as null. For example, if you want to consider a date column
with a value "1900-01-01" set null on DataFrame.

### dateFormat

`dateFormat` option to used to set the format of the input [DateType and
TimestampType]
columns. Supports all
[java.text.SimpleDateFormat](https://docs.oracle.com/javase/10/docs/api/java/time/format/DateTimeFormatter.html)
formats.

**Note:** Besides the above options, Spark CSV dataset also supports
many other options, [please refer to this article for
details](https://docs.databricks.com/data/data-sources/read-csv.html).

Reading CSV files with a user-specified custom schema
------------------------------------------------------------------------------------------------------------------------------------------------------

If you know the
[schema]
of the file ahead and do not want to use the `inferSchema` option for
column names and types, use user-defined custom column names and type
using `schema` option.

```
 
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
val df_with_schema = spark.read.format("csv")
      .option("header", "true")
      .schema(schema)
      .load("src/main/resources/zipcodes.csv")
df_with_schema.printSchema()
df_with_schema.show(false)
```



Applying DataFrame transformations
----------------------------------------------------------------------------------------------------------------

Once you have [created
DataFrame]
from the CSV file, you can apply all transformation and actions
DataFrame support. Please refer to the link for more details. 

Write Spark DataFrame to CSV file
--------------------------------------------------------------------------------------------------------------

Use the `write()` method of the Spark DataFrameWriter object to write
Spark DataFrame to a CSV file. For detailed example refer to [Writing
Spark DataFrame to CSV File using
Options].

```
 
df2.write.option("header","true")
 .csv("/tmp/spark_output/zipcodes")
```



### Options

While writing a CSV file you can use several options. for example,
`header` to output the DataFrame column names as header record and
`delimiter` to specify the delimiter on the CSV output file.

```
 
df2.write.options("header",true)
 .csv("/tmp/spark_output/zipcodes")
 
```



Other options
available `quote`,`escape`,`nullValue`,`dateFormat`,`quoteMode` .

### Saving modes

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
 
df2.write.mode(SaveMode.Overwrite).csv("/tmp/spark_output/zipcodes")
```



Conclusion:
-----------

In this tutorial, you have learned how to read a CSV file, multiple csv
files and all files from a local folder into Spark DataFrame, using
multiple options to change the default behavior and write CSV files back
to DataFrame using different save options.


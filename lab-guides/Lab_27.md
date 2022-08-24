
Spark Read from & Write to HBase table \| Example
=================================================




This tutorial explains how to read or load from and write Spark (2.4.X
version) DataFrame rows to HBase table using `hbase-spark` connector and
Datasource  `"org.apache.spark.sql.execution.datasources.hbase"` along
with Scala example.




Spark HBase library dependencies
--------------------------------

Below HBase libraries are required to connect Spark with the HBase
database and perform read and write rows to the table.

-   `hbase-client` This library provides by HBase which is used natively
    to interact with HBase.
-   `hbase-spark` connector which provides HBaseContext to interact
    Spark with HBase. HBaseContext pushes the configuration to the Spark
    executors and allows it to have an HBase Connection per Executor.

Below are complete maven dependencies to run the below examples in your
environment. Note that "hbase-client" has not provided as a dependency
since Spark HBase connector provides this as a transitive dependency and
this is a recommended way otherwise you may come across incompatibility
issues between these two.



```
  <dependencies>

    <dependency>
      <groupId>org.scala-lang</groupId>
      <artifactId>scala-library</artifactId>
      <version>2.11.12</version>
    </dependency>

    <dependency>
      <groupId>org.apache.spark</groupId>
      <artifactId>spark-core_2.11</artifactId>
      <version>2.4.0</version>
      <scope>compile</scope>
    </dependency>

    <dependency>
      <groupId>org.apache.spark</groupId>
      <artifactId>spark-sql_2.11</artifactId>
      <version>2.4.0</version>
      <scope>compile</scope>
    </dependency>

    <dependency>
      <groupId>org.apache.spark</groupId>
      <artifactId>spark-streaming_2.11</artifactId>
      <version>2.4.0</version>
    </dependency>

   <dependency>
     <groupId>org.apache.hbase.connectors.spark</groupId>
     <artifactId>hbase-spark</artifactId>
     <version>1.0.0</version>
   </dependency>

  </dependencies>
```

Writing Spark DataFrame to HBase table using "hbase-spark" connector
--------------------------------------------------------------------

First, let's create a DataFrame which we will store to HBase using
"hbase-spark" connector. In this snippet, we are creating an employee DF
with 3 rows.

```
case class Employee(key: String, fName: String, lName: String,
                      mName:String, addressLine:String, city:String,
                      state:String, zipCode:String)

 val data = Seq(Employee("1","Abby","Smith","K","3456 main", "Orlando","FL","45235"),
      Employee("2","Amaya","Williams","L","123 Orange","Newark", "NJ", "27656"),
      Employee("3","Alchemy","Davis","P","Warners","Sanjose","CA", "34789"))

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("sparkexamples")
      .getOrCreate()

    import spark.implicits._
    val df = spark.sparkContext.parallelize(data).toDF
```



Now, Let's define a catalog which bridges the gap between HBase KV store
and DataFrame table structure. using this we will also map the column
names between the two structures and keys. please refer below example
for the snippet. within the catalog, we also specify the HBase table we
are going to use and the namespace. here, we are using the "employee"
table in the "default" namespace.

```
def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"employee"},
         |"rowkey":"key",
         |"columns":{
         |"key":{"cf":"rowkey", "col":"key", "type":"string"},
         |"fName":{"cf":"person", "col":"firstName", "type":"string"},
         |"lName":{"cf":"person", "col":"lastName", "type":"string"},
         |"mName":{"cf":"person", "col":"middleName", "type":"string"},
         |"addressLine":{"cf":"address", "col":"addressLine", "type":"string"},
         |"city":{"cf":"address", "col":"city", "type":"string"},
         |"state":{"cf":"address", "col":"state", "type":"string"},
         |"zipCode":{"cf":"address", "col":"zipCode", "type":"string"}
         |}
         |}""".stripMargin
```



Finally, let's store the DataFrame to HBase table using save() function
on the data frame. from the below example, format
takes `"org.apache.spark.sql.execution.datasources.hbase"` DataSource
defined in "hbase-spark" API which enables us to use DataFrame with
HBase tables. And, `df.write.options` take the catalog and specifies to
use 4 regions in cluster. Finally, `save()`writes it to HBase table.

```
df.write.options(
      Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "4"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
```



Below is the complete example, and the same is available at
[GitHub](https://github.com/sparkbyexamples/spark-hbase-connector-examples/blob/master/src/main/scala/com/sparkbyexamples/spark/hbase/dataframe/HBaseSparkInsert.scala)

```
package com.sparkbyexamples.spark.hbase.dataframe

import org.apache.hadoop.hbase.spark.datasources.HBaseTableCatalog
import org.apache.spark.sql.SparkSession

object HBaseSparkInsert {

  case class Employee(key: String, fName: String, lName: String,
                      mName:String, addressLine:String, city:String,
                      state:String, zipCode:String)

  def main(args: Array[String]): Unit = {

    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"employee"},
         |"rowkey":"key",
         |"columns":{
         |"key":{"cf":"rowkey", "col":"key", "type":"string"},
         |"fName":{"cf":"person", "col":"firstName", "type":"string"},
         |"lName":{"cf":"person", "col":"lastName", "type":"string"},
         |"mName":{"cf":"person", "col":"middleName", "type":"string"},
         |"addressLine":{"cf":"address", "col":"addressLine", "type":"string"},
         |"city":{"cf":"address", "col":"city", "type":"string"},
         |"state":{"cf":"address", "col":"state", "type":"string"},
         |"zipCode":{"cf":"address", "col":"zipCode", "type":"string"}
         |}
         |}""".stripMargin


    val data = Seq(Employee("1","Abby","Smith","K","3456 main", "Orlando","FL","45235"),
      Employee("2","Amaya","Williams","L","123 Orange","Newark", "NJ", "27656"),
      Employee("3","Alchemy","Davis","P","Warners","Sanjose","CA", "34789"))

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("sparkexamples")
      .getOrCreate()

    import spark.implicits._
    val df = spark.sparkContext.parallelize(data).toDF

    df.write.options(
      Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "4"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }
}
```



If you use `scan 'employee'` on a shell, you will get the below 3 rows
as an output.

![Spark HBase
Connector]

Reading the table to DataFrame using "hbase-spark"
--------------------------------------------------

In this example, I will explain how to read data from the HBase table,
create a DataFrame and finally run some filters using DSL and SQL's.

Below is a complete example and it is also available at
[GitHub](https://github.com/sparkbyexamples/spark-hbase-connector-examples/blob/master/src/main/scala/com/sparkbyexamples/spark/hbase/dataframe/HBaseSparkRead.scala).

```
package com.sparkbyexamples.spark.hbase.dataframe

import org.apache.hadoop.hbase.spark.datasources.HBaseTableCatalog
import org.apache.spark.sql.SparkSession

object HBaseSparkRead {

  def main(args: Array[String]): Unit = {

    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"employee"},
         |"rowkey":"key",
         |"columns":{
         |"key":{"cf":"rowkey", "col":"key", "type":"string"},
         |"fName":{"cf":"person", "col":"firstName", "type":"string"},
         |"lName":{"cf":"person", "col":"lastName", "type":"string"},
         |"mName":{"cf":"person", "col":"middleName", "type":"string"},
         |"addressLine":{"cf":"address", "col":"addressLine", "type":"string"},
         |"city":{"cf":"address", "col":"city", "type":"string"},
         |"state":{"cf":"address", "col":"state", "type":"string"},
         |"zipCode":{"cf":"address", "col":"zipCode", "type":"string"}
         |}
         |}""".stripMargin

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("sparkexamples")
      .getOrCreate()

    import spark.implicits._

    // Reading from HBase to DataFrame
    val hbaseDF = spark.read
      .options(Map(HBaseTableCatalog.tableCatalog -> catalog))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    //Display Schema from DataFrame
    hbaseDF.printSchema()

    //Collect and show Data from DataFrame
    hbaseDF.show(false)

    //Applying Filters
    hbaseDF.filter($"key" === "1" && $"state" === "FL")
      .select("key", "fName", "lName")
      .show()

    //Create Temporary Table on DataFrame
    hbaseDF.createOrReplaceTempView("employeeTable")

    //Run SQL
    spark.sql("select * from employeeTable where fName = 'Amaya' ").show

  }
}
```



`hbaseDF.printSchema()` displays the below schema.

```
root
 |-- key: string (nullable = true)
 |-- fName: string (nullable = true)
 |-- lName: string (nullable = true)
 |-- mName: string (nullable = true)
 |-- addressLine: string (nullable = true)
 |-- city: string (nullable = true)
 |-- state: string (nullable = true)
 |-- zipCode: string (nullable = true)
```



`hbaseDF.show(false)` get the below data. Please note the DataFrame
field names differences with table column cell names.

```
+---+-------+--------+-----+-----------+-------+-----+-------+
|key|fName  |lName   |mName|addressLine|city   |state|zipCode|
+---+-------+--------+-----+-----------+-------+-----+-------+
|1  |Abby   |Smith   |K    |3456 main  |Orlando|FL   |45235  |
|2  |Amaya  |Williams|L    |123 Orange |Newark |NJ   |27656  |
|3  |Alchemy|Davis   |P    |Warners    |Sanjose|CA   |34789  |
+---+-------+--------+-----+-----------+-------+-----+-------+
```



###### Conclusion

In this tutorial, you have learned how the read from and write DataFrame
rows to HBase table using Spark HBase connector and Datasource
 `"org.apache.spark.sql.execution.datasources.hbase"` with Scala
example.

This complete project with Maven dependencies and many more HBase
examples are available at [GitHub "spark-hbase-connector-examples"
project](https://github.com/sparkbyexamples/spark-hbase-connector-examples)


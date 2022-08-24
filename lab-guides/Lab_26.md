

Create Spark DataFrame from HBase using Hortonworks
===================================================




This tutorial explains with a Scala example of how to create Spark
DataFrame from HBase table using Hortonworks DataSource
`"org.apache.spark.sql.execution.datasources.hbase"` from `shc-core`
library.

I would recommend reading [Inserting Spark DataFrame to HBase
table]
before you proceed to the rest of the lab where I explained Maven
dependencies and their usage.



Related: [Libraries and DataSource API's to connect Spark with
HBase]

In summary, To interact Spark DataFrame with HBase, you would need
`hbase-clinet,` `hbase-spark `and `shc-core` API's.



```
  <dependencies>
    <dependency>
      <groupId>org.apache.hbase</groupId>
      <artifactId>hbase-client</artifactId>
      <version>2.0.2.3.1.0.6-1</version> <!-- Hortonworks Latest -->
    </dependency>
    <dependency>
      <groupId>org.apache.hbase</groupId>
      <artifactId>hbase-spark</artifactId>
      <version>2.0.2.3.1.0.6-1</version> <!-- Hortonworks Latest -->
    </dependency>
    <dependency>
      <groupId>com.hortonworks</groupId>
      <artifactId>shc-core</artifactId>
      <version>1.1.1-2.1-s_2.11</version> <!-- Hortonworks Latest -->
    </dependency>
  </dependencies>
```

Below is the complete example, for your reference and the same example
is also available at
[GitHub](https://github.com/sparkbyexamples/spark-hbase-hortonworks-examples/blob/master/src/main/scala/com/sparkbyexamples/spark/dataframe/hbase/hortonworks/HBaseSparkRead.scala).

```
package com.sparkbyexamples.spark.dataframe.hbase.hortonworks

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.datasources.hbase.HBaseTableCatalog

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

    val sparkSession: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("sparkexamples")
      .getOrCreate()

    import sparkSession.implicits._

    val hbaseDF = sparkSession.read
      .options(Map(HBaseTableCatalog.tableCatalog -> catalog))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    hbaseDF.printSchema()

    hbaseDF.show(false)

    hbaseDF.filter($"key" === "1" && $"state" === "FL")
      .select("key", "fName", "lName")
      .show()

    //Create Temporary Table on DataFrame
    hbaseDF.createOrReplaceTempView("employeeTable")

    //Run SQL
    sparkSession.sql("select * from employeeTable where fName = 'Amaya' ").show

  }
}
```



Let me explain what's happening at a few statements in this example.

First, we need to define a catalog to bridge the gap between HBase KV
store and Spark DataFrame table structure. using this we will also map
the column names between the two structures and keys.



A couple of things happening at below snippet, format takes
`"org.apache.spark.sql.execution.datasources.hbase"` DataSource defined
in "shc-core" API which enables us to use DataFrame with HBase tables.
And, `df.read.options` take the catalog which we defined earlier.
Finally, `load()` reads the HBase table.

```
    val hbaseDF = sparkSession.read
      .options(Map(HBaseTableCatalog.tableCatalog -> catalog))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
```



`hbaseDF.printSchema()` displays the below schema.

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



hbaseDF.show(false) get the below data. Please note the DataFrame field
names differences with table column cell names.

```
+---+-------+--------+-----+-----------+-------+-----+-------+
|key|fName  |lName   |mName|addressLine|city   |state|zipCode|
+---+-------+--------+-----+-----------+-------+-----+-------+
|1  |Abby   |Smith   |K    |3456 main  |Orlando|FL   |45235  |
|2  |Amaya  |Williams|L    |123 Orange |Newark |NJ   |27656  |
|3  |Alchemy|Davis   |P    |Warners    |Sanjose|CA   |34789  |
+---+-------+--------+-----+-----------+-------+-----+-------+
```



`hbaseDF.filter()` filter the data using DSL functions.

```
+---+-----+-----+
|key|fName|lName|
+---+-----+-----+
|  1| Abby|Smith|
+---+-----+-----+
```



finally, we can create a temporary SQL table and run all SQL queries.

```
+---+-----+--------+-----+-----------+------+-----+-------+
|key|fName|   lName|mName|addressLine|  city|state|zipCode|
+---+-----+--------+-----+-----------+------+-----+-------+
|  2|Amaya|Williams|    L| 123 Orange|Newark|   NJ|  27656|
+---+-----+--------+-----+-----------+------+-----+-------+
```



###### Conclusion:

In this tutorial, you have learned how to create Spark DataFrame from
HBase table using Hortonworks DataSource API and also have seen how to
run DSL and SQL queries on Hbase DataFrame.

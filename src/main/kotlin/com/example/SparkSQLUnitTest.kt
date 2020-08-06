package com.example

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession

class TestClass {
    fun run() {
        val warehouseLocation = createTempDir()
        println("++++ warehouseLocation=$warehouseLocation")
        val spark: SparkSession = SparkSession
            .builder()
            .appName("Java Spark SQL basic example") // your application name
            .config("spark.master", "local")  // run on local machine, single thread.
            .config("spark.sql.warehouse.dir", warehouseLocation.toString())
            .config("spark.ui.enabled", false)
            .enableHiveSupport()
            .getOrCreate()

        val resourcePath = javaClass.classLoader.getResource("test-data/people.json")!!.toString()
        println("++++ read csv from: $resourcePath")

        val df = spark.read()
            .json(resourcePath)
        df.show()
        df.printSchema()

        println("++++ create table")
        spark.sql("create database if not exists foo")
        df.write().mode(SaveMode.Overwrite).saveAsTable("foo.people")
        spark.sql("show tables").show()
        spark.sql("show create table foo.people").show(false)

        // If the type of data is the important thing, you need to write your schema by yourself.
        //        spark.sql("""drop table if exists `foo`.`people`""")
//        spark.sql("""
//        CREATE TABLE `foo`.`people` (
//            `name` STRING,
//            `age` long,
//            `extra_fields` STRING)
//        USING parquet""".trimIndent())
//        df.write().insertInto("foo.people")


        println("++++ select")
        val sqlDF: Dataset<Row> = spark.sql("SELECT * FROM foo.people")
        sqlDF.show(false)

        println("++++ select 2nd")
        val sqlDF2: Dataset<Row> = spark.sql("SELECT name, get_json_object(extra_fields, '$.interests') interests FROM foo.people")
        sqlDF2.show()

        println("++++ select 3rd")
        val sqlDF3: Dataset<Row> = spark.sql("SELECT avg(age) avg_age FROM foo.people")
        sqlDF3.show()
    }
}

fun main() {
    TestClass().run()
}

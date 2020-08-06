package com.example

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import java.lang.management.ManagementFactory

// test without
class SimpleTest {
    fun run() {
        val spark: SparkSession = SparkSession
            .builder()
            .appName("Java Spark SQL basic example") // your application name
            .config("spark.master", "local")  // run on local machine, single thread.
            .config("spark.ui.enabled", false)
            .getOrCreate()

        val resourcePath = javaClass.classLoader.getResource("test-data/people.json")!!.toString()
        println("++++ read csv from: $resourcePath")

        val df = spark.read()
            .json(resourcePath)
        df.show()
        df.printSchema()

        println("++++ create table")
        df.createTempView("people")

        println("++++ select")
        val sqlDF: Dataset<Row> = spark.sql("SELECT * FROM people")
        sqlDF.show(false)

        println("++++ select 2nd")
        val sqlDF2: Dataset<Row> = spark.sql("SELECT name, get_json_object(extra_fields, '$.interests') interests FROM people")
        sqlDF2.show()

        println("++++ select 3rd")
        val sqlDF3: Dataset<Row> = spark.sql("SELECT avg(age) avg_age FROM people")
        sqlDF3.show()
    }
}

fun main() {
    SimpleTest().run()
    println("\n\n\n++++ Elapsed: ${ManagementFactory.getRuntimeMXBean().uptime}")
}

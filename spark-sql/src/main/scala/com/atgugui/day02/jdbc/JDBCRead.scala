package com.atgugui.day02.jdbc

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
  * Author lzc
  * Date 2019-09-21 09:09
  */
object JDBCRead {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("JDBCRead")
            .getOrCreate()
        import spark.implicits._
        
        // 通用的读jdbc
        /*val df = spark.read.format("jdbc")
            .option("url", "jdbc:mysql://hadoop201:3306/rdd")
            .option("user", "root")
            .option("password", "aaa")
            .option("dbtable", "user")
            .load()*/
        
        // 专用的读jdbc
        val props = new Properties()
        props.setProperty("user", "root")
        props.setProperty("password", "aaa")
        val df = spark.read.jdbc(
            "jdbc:mysql://hadoop201:3306/rdd",
            "user",
            props)
        
        df.createTempView("user")
        spark.sql("select * from user").show(100)
        
        spark.close()
    }
}

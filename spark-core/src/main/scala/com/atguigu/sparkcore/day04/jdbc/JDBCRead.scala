package com.atguigu.sparkcore.day04.jdbc

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-09-17 15:30
  */
object JDBCRead {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        //定义连接mysql的参数
        val driver = "com.mysql.jdbc.Driver"
        val url = "jdbc:mysql://hadoop201:3306/rdd"
        val userName = "root"
        val passWd = "aaa"
    
    
        val rdd = new JdbcRDD(
            sc,
            () => {
                Class.forName(driver)
                DriverManager.getConnection(url, userName, passWd)
            },
            "select * from user where ? <= id and id <= ?",
            20,
            60,
            2,
            result => result.getInt(1)
        )
        
        rdd.collect.foreach(println)
        
        sc.stop()
        
    }
}

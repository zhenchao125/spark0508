package com.atguigu.sparkcore.day03.action

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-09-16 15:31
  */
object Foreach {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(Array("hello", "hello", "world", "hello", "atguigu", "hello", "atguigu", "atguigu"))
        
        //        rdd1.foreach(println)
        rdd1.foreachPartition(it => {
            // 连接一次到jdbc
            
            
        })
        
        sc.stop()
        
    }
}

/*
foreach 可以用来向外界写出数据: jdbc, hive, hdfs, hbase ....
 */
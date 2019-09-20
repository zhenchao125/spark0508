package com.atguigu.sparkcore.day03.action

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-09-16 15:21
  */
object CountByKey {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(Array("hello", "hello", "world", "hello", "atguigu", "hello", "atguigu", "atguigu"))
        var rdd3 = rdd1.map( x => {
            println(x)
            (x, 2)
        })
//        val rdd4 = rdd3.reduceByKey(_ + _)
//
//        println("----")
//        rdd4.collect
        val map = rdd3.countByKey()
        println(map)
        sc.stop()
        
    }
}
/*
countByKey 是行动算子
reduceByKey 是转换算子
 */
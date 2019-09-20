package com.atguigu.sparkcore.day03.action

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-09-16 16:24
  */
object Question {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(Array("hello", "hello", "world", "hello", "atguigu", "hello", "atguigu", "atguigu"))
        
        var rdd2 = rdd1.map(x => {
            println(x)
            (x, 1)
        }).reduceByKey(_ + _).sortByKey()
        sc.stop()
    }
}

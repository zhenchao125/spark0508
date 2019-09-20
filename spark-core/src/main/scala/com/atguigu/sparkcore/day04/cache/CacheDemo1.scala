package com.atguigu.sparkcore.day04.cache

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-09-17 10:29
  */
object CacheDemo1 {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(Array("hello", "hello", "world", "hello", "atguigu", "hello", "atguigu", "atguigu"))
        val rdd2 = rdd1.map(x => {
            println(x)
            (x, 1)
        })
        val rdd4 = rdd2.reduceByKey(_ + _)
        // 对rdd4计算的结果做缓存
//        rdd4.cache()  // 缓存到内存中
//        rdd4.persist()
        rdd4.collect
        println("-------")
        rdd4.collect
        
        Thread.sleep(1000000000)
        sc.stop()
    }
}

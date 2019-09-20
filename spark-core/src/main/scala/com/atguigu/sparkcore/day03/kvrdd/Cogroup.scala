package com.atguigu.sparkcore.day03.kvrdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-09-16 14:05
  */
object Cogroup {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        var rdd1 = sc.parallelize(Array((1, "a"), (1, "b"), (2, "c"), (4, "xx")))
        val rdd2 = sc.parallelize(Array((1, "aa"), (3, "bb"), (2, "cc"), (2, "dd")))
        var rdd3 = rdd1.cogroup(rdd2)
        rdd3.collect.foreach(println)
        sc.stop()
        
    }
}

/*
a join b on a.x=b.x

 */

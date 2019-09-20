package com.atguigu.sparkcore.day03.action

import org.apache.spark.{SparkConf, SparkContext}

object Reduce {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd2 = sc.parallelize(Array(3, 5, 8, 2, 1, 9, 5, 7), 3)
//        val result = rdd2.reduce(_ + _)
//        val result: Int = rdd2.fold(0)(_ + _)
//        val result = rdd2.aggregate(0)(_ + _, _ + _)
        // zero 参与运算的次数是: 分区数+1   (每个分区使用一次, 分区间进行合并的时候再使用一次)
        val result = rdd2.aggregate(10)(_ + _, _ + _)
        println(result)
        sc.stop()
        
    }
}

/*

 */
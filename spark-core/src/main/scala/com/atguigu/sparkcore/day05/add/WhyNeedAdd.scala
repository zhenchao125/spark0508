package com.atguigu.sparkcore.day05.add

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-09-17 17:53
  */
object WhyNeedAdd {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("WhyNeedAdd").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val rdd2: RDD[Int] = sc.parallelize(Array(3, 5, 8, 2))
        
        var a = 1
        val rdd3 = rdd2.map(x => {
            a += 1
            println(a)
            (x, 1)
        })
        rdd3.collect
        println("------")
        println(a)  // 1
        sc.stop()
    }
}

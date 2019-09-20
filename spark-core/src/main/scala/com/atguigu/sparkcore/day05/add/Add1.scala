package com.atguigu.sparkcore.day05.add

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-09-18 09:11
  */
object Add1 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Add1").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val rdd2: RDD[Int] = sc.parallelize(Array(3, 5, 8, 2))
//        val acc: LongAccumulator = sc.longAccumulator
        
        val acc = new MyAcc
        // 要向sc注册自定义的累加器
        sc.register(acc)
        val rdd3 = rdd2.map(x => {
            acc.add(1)
            (x, 1)
        })
        
        
        rdd3.collect
        println("------")
        println(acc.value)  //
        sc.stop()
        
    }
}
/*
累加器解决了什么问题:
    解决了共享变量写的问题
 */
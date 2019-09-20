package com.atguigu.sparkcore.day05.add

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-09-18 09:11
  */
object Add2 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Add1").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val acc = new MapAcc
        sc.register(acc)
        val rdd2 = sc.parallelize(Array(3, 5, 8, 2, 1, 9, 5, 7))
        // sum, avg, max, min
        rdd2.foreach(x => acc.add(x))
    
        println(acc.value)
        sc.stop()
        
        
    }
}

/*
累加器解决了什么问题:
    解决了共享变量写的问题
    
用累加器的时候:
    累加器只在行动算子中使用, 不要在转换算子使用
 */
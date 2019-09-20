package com.atguigu.sparkcore.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-09-11 16:29
  */
object Wordcount {
    def main(args: Array[String]): Unit = {
        // 1. 初始化spark context
        val conf: SparkConf = new SparkConf()
            .setAppName("Wordcount")
        val sc: SparkContext = new SparkContext(conf)
        sc.setLogLevel("error")
        // 2. 各种转换
        val rdd: RDD[(String, Int)] = sc
            .textFile(args(0))
            .flatMap(_.split(","))
            .map((_, 1))
            .reduceByKey(_ + _)
        
        // 3. 行动
        val result: Array[(String, Int)] = rdd.collect()
        result.foreach(println)
        
        // 4. 关闭上下文
        sc.stop()
    }
}

package com.atguigu.spark.streaming.day01

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * Author lzc
  * Date 2019-09-23 09:29
  */
object WordCount2 {
    def main(args: Array[String]): Unit = {
        
        val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount2")
        val ssc = new StreamingContext(conf, Seconds(3))
        
        val rddQueue = mutable.Queue[RDD[Int]]()
        // 测试
        val resultDStream = ssc.queueStream(rddQueue, false)
            .reduce(_ + _)
        resultDStream.print
        
        ssc.start()
        while (true) {
            rddQueue.enqueue(ssc.sparkContext.parallelize(1 to 100))
            Thread.sleep(2000)
        }
        ssc.awaitTermination()
    }
}
/*
1.socket

2. rdd队列
 


 */
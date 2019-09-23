package com.atguigu.spark.streaming.day01.unstate

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author lzc
  * Date 2019-09-23 16:14
  */
object TransformDemo {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HighKafka")
        val ssc = new StreamingContext(conf, Seconds(3))
    
        val socketStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop201", 9999)
        val resultDSteam = socketStream.transform(rdd => {
            rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
        })
        resultDSteam.print
        
        ssc.start()
        ssc.awaitTermination()
        
    }
}

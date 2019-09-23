package com.atguigu.spark.streaming.day01

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author lzc
  * Date 2019-09-23 09:02
  */
object WordCount1 {
    def main(args: Array[String]): Unit = {
        // 1. 创建SteamingContext
        val conf = new SparkConf().setMaster("local[2]").setAppName("WordCount1")
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(4))
        // 2. 核心数据集: DStreaming
        val socketStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop201", 9999)
        // 3. 对DSteaming做各种操作
        val wordCountDStream: DStream[(String, Int)] =
            socketStream
                .flatMap(_.split(" "))
                .map((_, 1))
                .reduceByKey(_ + _)
        
        // 4. 最终数据的处理: 打印
        wordCountDStream.print(100)
        // 5. 启动 teamingContext
        ssc.start()
        // 6. 阻止当前线程退出
        ssc.awaitTermination()
    }
}

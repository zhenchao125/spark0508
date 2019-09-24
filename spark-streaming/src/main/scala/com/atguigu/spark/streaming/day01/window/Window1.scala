package com.atguigu.spark.streaming.day01.window

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author lzc
  * Date 2019-09-23 09:29
  */
object Window1 {
    def main(args: Array[String]): Unit = {
        
        val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount2")
        val ssc = new StreamingContext(conf, Seconds(4))
        val sourceDSteram: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop201", 9999)
        sourceDSteram.flatMap(_.split(" ")).map((_, 1))
            //            .reduceByKeyAndWindow(_ + _, Seconds(12))  // 默认滑动步长是周期
            .reduceByKeyAndWindow((_: Int) + (_:Int), Seconds(12), Seconds(8)) // 使用自定义的步长
            .print(10)
        
        ssc.start()
        ssc.awaitTermination()
    }
}

/*


 */
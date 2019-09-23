package com.atguigu.spark.streaming.day01.unstate

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author lzc
  * Date 2019-09-23 16:14
  */
object WithStateDemo {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HighKafka")
        val ssc = new StreamingContext(conf, Seconds(3))
        ssc.checkpoint("./ck2")
//        ssc.sparkContext.setCheckpointDir("./ck3")
        val socketStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop201", 9999)
        val resultDSteam = socketStream
            .flatMap(_.split(" "))
            .map((_, 1))
            .updateStateByKey[Int]((seq: Seq[Int], opt: Option[Int]) => Some(seq.sum + opt.getOrElse(0)))
        
        
        resultDSteam.print
        
        ssc.start()
        ssc.awaitTermination()
        
    }
}

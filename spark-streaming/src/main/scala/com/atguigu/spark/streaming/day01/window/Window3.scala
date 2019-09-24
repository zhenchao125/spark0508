package com.atguigu.spark.streaming.day01.window

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author lzc
  * Date 2019-09-23 09:29
  */
object Window3 {
    def main(args: Array[String]): Unit = {
        
        val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount2")
        val ssc = new StreamingContext(conf, Seconds(4))
        ssc.checkpoint("./ck4")
        
        val spark = SparkSession.builder().getOrCreate()
        val sourceDSteram = ssc.socketTextStream("hadoop201", 9999)
        sourceDSteram.flatMap(_.split(" "))
            .map((_, 1))
            .reduceByKeyAndWindow((_: Int) + (_: Int), (_: Int) - (_: Int), Seconds(12), Seconds(4), filterFunc = _._2 > 0
        )
        .print
        
        
        ssc.start()
        ssc.awaitTermination()
    }
}

/*


 */
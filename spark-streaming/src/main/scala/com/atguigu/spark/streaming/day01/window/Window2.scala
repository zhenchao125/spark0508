package com.atguigu.spark.streaming.day01.window

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author lzc
  * Date 2019-09-23 09:29
  */
object Window2 {
    def main(args: Array[String]): Unit = {
        
        val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount2")
        val ssc = new StreamingContext(conf, Seconds(4))
        
        
        val spark = SparkSession.builder().getOrCreate()
        import spark.implicits._
        val sourceDSteram = ssc.socketTextStream("hadoop201", 9999).window(Seconds(12), Seconds(8))
        sourceDSteram.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
                .transform(rdd => {
                    val df = rdd.toDF("word", "count")
                    df.createOrReplaceTempView("w")
                    spark.sql("select count from w").rdd
                })
                .print
            /*.foreachRDD(rdd => {
                rdd.foreachPartition(it => {
                    // 连接
                    it.foreach(x => {
                    
                    })
                    
                    // 关闭连接
                })
            })*/
        
        
        ssc.start()
        ssc.awaitTermination()
    }
}

/*


 */
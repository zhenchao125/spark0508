package com.atguigu.spark.streaming.day01.kafka

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author lzc
  * Date 2019-09-23 11:38
  */
object WordCount2 {
    
    def createSSc(): StreamingContext = {
        println("aaaa")
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HighKafka")
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
        ssc.checkpoint("./ck1")
        // kafka 参数
        //kafka参数声明
        val brokers = "hadoop201:9092,hadoop202:9092,hadoop203:9092"
        val topic = "first"
        val group = "bigdata"
        val kafkaParams = Map(
            ConsumerConfig.GROUP_ID_CONFIG -> group,
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers
        )
    
        // 泛型12: kev,vlaue的类型   泛型34: keyvalue的解码器
        val sourceDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
            ssc,
            kafkaParams,
            Set(topic))
        sourceDStream.print
        
        ssc
        
    }
    
    def main(args: Array[String]): Unit = {
        
        val ssc: StreamingContext = StreamingContext.getActiveOrCreate("./ck1", createSSc)
        
        ssc.start()
        ssc.awaitTermination()
        
    }
}

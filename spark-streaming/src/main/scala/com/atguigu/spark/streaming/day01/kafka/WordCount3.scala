package com.atguigu.spark.streaming.day01.kafka

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaCluster.Err
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author lzc
  * Date 2019-09-23 11:38
  */
object WordCount3 {
    val brokers = "hadoop201:9092,hadoop202:9092,hadoop203:9092"
    val topic = "first"
    val group = "bigdata"
    val kafkaParams = Map(
        ConsumerConfig.GROUP_ID_CONFIG -> group,
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers
    )
    
    // 手动提交offset,读取offsets需要使用
    val kafkaCluster: KafkaCluster = new KafkaCluster(kafkaParams)
    
    /*
    读取offsets
     */
    def readOffsets(): Map[TopicAndPartition, Long] = {
        var reslutMap = Map[TopicAndPartition, Long]()
        // 获取到所有的分区
        val topicAndPartitionEither: Either[Err, Set[TopicAndPartition]] = kafkaCluster.getPartitions(Set(topic))
        topicAndPartitionEither match {
            case Right(topicAndPationSet) =>
                // 分区存在, 就获取 分区和偏移量
                val topicAndPatitionOffsetEither: Either[Err, Map[TopicAndPartition, Long]] = kafkaCluster.getConsumerOffsets(group, topicAndPationSet)
                if (topicAndPatitionOffsetEither.isRight) { // 表示曾经消费过, 有offset
                    val topicAndPartitionOffsetMap: Map[TopicAndPartition, Long] = topicAndPatitionOffsetEither.right.get
                    reslutMap ++= topicAndPartitionOffsetMap
                } else { // 表示第一次消费分区  把每个分区的偏移量置为0
                    topicAndPationSet.foreach(tap => {
                        reslutMap += tap -> 0L
                    })
                }
            
            case _ =>
        }
        reslutMap
    }
    
    /*
    提交offsets
     */
    def writeOffsets(sourceDStream: InputDStream[String]) = {
        sourceDStream.foreachRDD(rdd => {
            var map = Map[TopicAndPartition, Long]()
            // 强转成HasOffsetRanges, 包含了本次消费的offset起始范围
            val hasOffsetRanges: HasOffsetRanges = rdd.asInstanceOf[HasOffsetRanges]
            val rangs: Array[OffsetRange] = hasOffsetRanges.offsetRanges
            rangs.foreach(rang => {
                val offset: Long = rang.untilOffset
                map += rang.topicAndPartition() -> offset
            })
            kafkaCluster.setConsumerOffsets(group, map)
        })
    }
    
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HighKafka")
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
        
        
        val offsets = readOffsets()
        val sourceDStream: InputDStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](
            ssc,
            kafkaParams,
            readOffsets(),
            (mm: MessageAndMetadata[String, String]) => mm.message()
        )
        sourceDStream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).print(100)
        writeOffsets(sourceDStream)
        
        ssc.start()
        ssc.awaitTermination()
    }
}

package com.atguigu.sparkcore.day04.checkpoint

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-09-17 11:11
  */
object Checkpoint {
    System.setProperty("HADOOP_USER_NAME", "atguigu")
    
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        
        val sc = new SparkContext(conf)
        sc.setCheckpointDir("hdfs://hadoop201:9000/ck0508")
        
        val rdd1 = sc.parallelize(Array("hello"))
        val rdd4 = rdd1.map(x => {
            
            (x, System.currentTimeMillis())
        }).reduceByKey(_ + _)
        
        rdd4.cache()
        rdd4.checkpoint() // 针对rdd4做checkpoint
        rdd4.collect.foreach(println)
        // 会立即启动一个新的job来专门的去做checkpoint
        println("-------")
        rdd4.collect.foreach(println)
        rdd4.collect.foreach(println)
        rdd4.collect.foreach(println)
        
        
        Thread.sleep(1000000000)
        sc.stop()
        
    }
}

/*
cache  checkpoint

cache:
 1. 会使用以及计算好的结果直接做缓存
 2. 仍然记录着他的血缘关系
 
checkpoint
 1. 会自动启动一个新的job,来做checkpoint
 2. 会切断他的血缘关系
 */
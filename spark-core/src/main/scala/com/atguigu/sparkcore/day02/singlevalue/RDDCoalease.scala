package com.atguigu.sparkcore.day02.singlevalue

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-09-12 13:51
  */
object RDDCoalease {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(Array(1, 2, 3, 4, 5, 6), 4)
        println(rdd1.getNumPartitions)
        //        val rdd2: RDD[Int] = rdd1.coalesce(6)
        val rdd2 = rdd1.repartition(5)
        rdd2.glom().collect.foreach(x => println(x.mkString(", ")))
        sc.stop()
        
    }
}

/*
coalesce 默认是只能减少分区, 而且这个减少不会shuffle
    减少

repartition 可以增也可以减, 一定会shuffle
    增加
 */
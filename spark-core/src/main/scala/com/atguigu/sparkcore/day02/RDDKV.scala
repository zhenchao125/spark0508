package com.atguigu.sparkcore.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-09-12 16:15
  */
object RDDKV {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1: RDD[(Int, Int)] = sc.parallelize(Array((1, 1), (2, 1), (3, 1), (4, 1), (5, 1), (6, 1)))
        val rdd2 = rdd1.partitionBy(new HashPartitioner(2))
        
        rdd2.glom().collect.foreach(arr => println(arr.mkString(", ")))
        
    }
}

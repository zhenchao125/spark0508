package com.atguigu.sparkcore.day02.doublevalue

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-09-12 15:19
  */
object RDDVV {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1 = sc.parallelize(Array(30, 50, 70, 61, 10, 20, 1, 2, 3), 2)
        val rdd2 = sc.parallelize(Array(30, 10, 70, 4, 10, 10), 2)
        
        
        //        val rdd3: RDD[Int] = rdd1.union(rdd2)
        //        val rdd3: RDD[Int] = rdd1 ++ rdd2
        
        //        val rdd3 = rdd1.subtract(rdd2)
        
        //        val rdd3: RDD[Int] = rdd1.intersection(rdd2)
        //        val rdd3 = rdd1.cartesian(rdd2)
        //        println(rdd3.getNumPartitions)
        
        /*
        1. 分区数相同
        2. 对应的分区内的元素数也要相同(两个rdd元素数相同)
         */
        //        val rdd3 = rdd1.zip(rdd2)
        //        val rdd3 = rdd1.zipWithIndex()  // 元素和他的索引进行zip
        val rdd3 = rdd1.zipPartitions(rdd2)((it1, it2) => {
            it1.zipAll(it2, 100, 200).zipWithIndex
        })
        rdd3.glom().collect.foreach(arr => println(arr.mkString(", ")))
        sc.stop()
    }
}

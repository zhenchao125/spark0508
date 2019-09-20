package com.atguigu.sparkcore.day04.readwrite

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-09-17 14:58
  */
object ReadFile {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val lines = sc.textFile("c:/0508")
        lines.collect.foreach(println)
        sc.stop()
        
    }
}

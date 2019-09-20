package com.atguigu.sparkcore.day05.broadcast

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-09-18 10:37
  */
object WhyNeedBD {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("WhyNeedBD").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val rdd2 = sc.parallelize(Array(3, 5, 8, 20, 1, 9, 5, 7), 4)
        
        // 发送广播变量: 发送到executor上
        val bd = sc.broadcast(Set(10, 20))
        
        rdd2.foreach(x => {
            // 取出广播变量中存储的值
            val set: Set[Int] = bd.value
            println(set.contains(x))
        })
        
        sc.stop()
        
    }
}
/*
解决变量的共享:

    累加器
        写
    

广播变量:
    广播变量不是直接发给task,而是发到 Executor 上
    
    读
    

 */
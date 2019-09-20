package com.atguigu.project.app

import com.atguigu.project.bean.{CategoryCountInfo, UserVisitAction}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-09-18 11:34
  */
object ProjectApp {
    def main(args: Array[String]): Unit = {
        // 1. sc初始化好
        val conf: SparkConf = new SparkConf().setAppName("ProjectApp").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        // 2. 把数据读取出来
        val lineRDD: RDD[String] = sc.textFile("c:/user_visit_action.txt")
        // 3. 封装到样例类中
        val userVisitActionRDD: RDD[UserVisitAction] = lineRDD.map(line => {
            val splits: Array[String] = line.split("_")
            UserVisitAction(
                splits(0),
                splits(1).toLong,
                splits(2),
                splits(3).toLong,
                splits(4),
                splits(5),
                splits(6).toLong,
                splits(7).toLong,
                splits(8),
                splits(9),
                splits(10),
                splits(11),
                splits(12).toLong)
        })
        
        // 需求1: top10的热门品类
//        val top10CategoryCountInfo: List[CategoryCountInfo] = CategoryTop10App.statCategoryTop10(sc, userVisitActionRDD)
        // 需求2:
//        CategorySessionTopApp.statCategoryTop10Session(sc, userVisitActionRDD, top10CategoryCountInfo)
//        println("--------")
//        CategorySessionTopApp.statCategoryTop10Session_2(sc, userVisitActionRDD, top10CategoryCountInfo)
        // 需求3:
        PageConversionApp.calcPageConversion(sc,userVisitActionRDD, "1,2,3,4,5,6,7")
        sc.stop()
    }
}

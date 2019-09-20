package com.atguigu.project.app

import com.atguigu.project.bean.{CategoryCountInfo, CategorySession, UserVisitAction}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkContext}

import scala.collection.mutable

/**
  * Author lzc
  * Date 2019-09-18 15:12
  */
object CategorySessionTopApp {
    def statCategoryTop10Session(sc: SparkContext, userVisitActionRDD: RDD[UserVisitAction], top10CategoryCountInfo: List[CategoryCountInfo]) = {
        // 1. 包含top10cid的那些用户点击记录过滤出来
        val filteredUserVisitActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(action => {
            top10CategoryCountInfo.map(_.categoryId).contains(action.click_category_id.toString)
        })
        //RDD[(cid, sid), 1]
        val cidSidOneRDD = filteredUserVisitActionRDD.map(action => ((action.click_category_id, action.session_id), 1))
        
        // RDD[(cid, sid), count]    RDD[(cid, (sid, count)]  RDD[(cid, Iteraot[(sid, count)])]
        val cidSidCountItRDD: RDD[(Long, Iterable[(String, Int)])] = cidSidOneRDD
            .reduceByKey(_ + _)
            .map {
                case ((cid, sid), count) => (cid, (sid, count))
            }
            .groupByKey
        
        
        // 排序, 取10
        val resultRDD = cidSidCountItRDD.map {
            case (cid, sidCountIt) => (cid, sidCountIt.toList.sortBy(-_._2).take(10))
        }
        
        resultRDD.collect.foreach(println)
    }
    
    def statCategoryTop10Session_1(sc: SparkContext, userVisitActionRDD: RDD[UserVisitAction], top10CategoryCountInfo: List[CategoryCountInfo]) = {
        // 1. 包含top10cid的那些用户点击记录过滤出来
        val filteredUserVisitActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(action => {
            top10CategoryCountInfo.map(_.categoryId).contains(action.click_category_id.toString)
        })
        //RDD[(cid, sid), 1]
        val cidSidOneRDD = filteredUserVisitActionRDD.map(action => ((action.click_category_id, action.session_id), 1))
        
        // RDD[(cid, sid), count]    RDD[(cid, (sid, count)]  RDD[(cid, Iteraot[(sid, count)])]
        val cidSidCountItRDD: RDD[(Long, Iterable[(String, Int)])] = cidSidOneRDD
            .reduceByKey(_ + _)
            .map {
                case ((cid, sid), count) => (cid, (sid, count))
            }
            .groupByKey
        
        
        top10CategoryCountInfo.map(_.categoryId).foreach(cid => {
            val onlyCidRDD: RDD[(Long, Iterable[(String, Int)])] = cidSidCountItRDD.filter(_._1.toString == cid)
            val sidCountRDD: RDD[(String, Int)] = onlyCidRDD.flatMap {
                case (_, it) => it
            }
            val result: Array[CategorySession] = sidCountRDD
                .sortBy(_._2, ascending = false)
                .take(10)
                .map {
                    case (sid, count) => CategorySession(cid, sid, count)
                }
            
            // 写入外部存储: jdbc, hive, hbase...
            result.foreach(println)
            println("--------")
        })
        
    }
    
    def statCategoryTop10Session_2(sc: SparkContext, userVisitActionRDD: RDD[UserVisitAction], top10CategoryCountInfo: List[CategoryCountInfo]) = {
        // 1. 包含top10cid的那些用户点击记录过滤出来
        val filteredUserVisitActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(action => {
            top10CategoryCountInfo.map(_.categoryId).contains(action.click_category_id.toString)
        })
        //RDD[(cid, sid), 1]
        val cidSidOneRDD: RDD[((Long, String), Int)] = filteredUserVisitActionRDD.map(action => ((action.click_category_id, action.session_id), 1))
        
        // RDD[(cid, sid), count]    RDD[(cid, (sid, count)]  RDD[(cid, Iteraot[(sid, count)])]
        val cidSidCountItRDD: RDD[CategorySession] = cidSidOneRDD
            .reduceByKey(new MyPartitioner(top10CategoryCountInfo.map(_.categoryId)), _ + _) // 重新分区
            .map {
                case ((cid, sid), count) => CategorySession(cid.toString, sid, count)
            }
        val resultRdd = cidSidCountItRDD.mapPartitions(it => {
            var treeSet: mutable.TreeSet[CategorySession] = new mutable.TreeSet[CategorySession]()
            it.foreach(cs => {
                treeSet += cs
                if (treeSet.size > 10) {
                    treeSet = treeSet.take(10)
                }
            })
            treeSet.toIterator
        })
        
        resultRdd.collect.foreach(println)
    }
}

// 根据cid的个数进行分区
class MyPartitioner(categoryIdTop10: List[String]) extends Partitioner {
    private val cidIndexList: Map[String, Int] = categoryIdTop10.zipWithIndex.toMap
    
    override def numPartitions: Int = categoryIdTop10.size
    
    override def getPartition(key: Any): Int = {
        key match {
            case (cid: Long, _) => cidIndexList(cid.toString)
        }
    }
}

/*
存在的问题: 内存溢出  sidCountIt.toList

1. rdd 全局排序没有内存溢出, shuffle, 依赖磁盘
       优点: 不会内存溢出
       缺点: 启动比较多的job
       
2.









top10品类中, 每个品类的top10活跃session

1. 只过滤出来包含top10 的点击记录

2. ...

=> RDD[(cid, sid), 1]	reduceByKey
=> RDD[(cid, sid), count]			map
=> RDD[(cid, (sid, count)]		groupByKey
RDD[(cid, Iteraot[(sid, count)])]  map
 */
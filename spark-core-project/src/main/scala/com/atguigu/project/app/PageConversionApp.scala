package com.atguigu.project.app

import java.text.DecimalFormat

import com.atguigu.project.bean.UserVisitAction
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Author lzc
  * Date 2019-09-20 09:20
  */
object PageConversionApp {
    // "1,2,3,4"
    def calcPageConversion(sc: SparkContext, userVisitActionRDD: RDD[UserVisitAction], pages: String) = {
        // 1. 得到目标跳转流
        val splits: Array[String] = pages.split(",")
        val prePages = splits.slice(0, splits.length - 1)
        val postPages = splits.slice(1, splits.length)
        // 试着添加广播变量
        val pageFlow = prePages.zip(postPages).map {
            case (pre, post) => pre + "->" + post
        }
        
        // 2. 计算每个目标页面的点击量
        val targetPageCount: collection.Map[Long, Long] = userVisitActionRDD
            .filter(action => prePages.contains(action.page_id.toString))
            .map(action => (action.page_id, 1))
            .countByKey()
        
        // 3. 计算每跳转流的数量
        val totalPageFlows: collection.Map[String, Long] = userVisitActionRDD
            //            .filter(action => splits.contains(action.page_id.toString))  // 先过滤出来只包含目标页面的  有问题, 不要过滤
            .groupBy(_.session_id)
            .flatMap {
                case (_, actionIt) => {
                    // 1->2,  2->3
                    val list: List[UserVisitAction] = actionIt.toList.sortBy(_.action_time) // 按照时间排序
                    val preActions: List[UserVisitAction] = list.slice(0, list.length - 1)
                    val postActions: List[UserVisitAction] = list.slice(1, list.length)
                    val totalPageFlows: List[String] = preActions.zip(postActions).map {
                        case (preAction, postAction) => preAction.page_id + "->" + postAction.page_id
                    }
                    
                    totalPageFlows.filter(flow => pageFlow.contains(flow)).map((_, 1))
                }
            }
            .countByKey()
        
        // 4. 最后计算跳转率
        
        val result: collection.Map[String, String] = totalPageFlows.map {
            // 1->2
            case (flow, flowCount) =>
                val page = flow.split("->")(0)
                val rate = flowCount.toDouble / targetPageCount.getOrElse(page.toLong, Long.MaxValue)
                val formater = new DecimalFormat(".00%")
                (flow, formater.format(rate))
        }
        println(result)
        
    }
}

/*
1. 知道需要计算哪些页面跳转率
	"1,2,3,4,5,6,7"
	做出来一个跳转流
	
	"1->2", "2->3"
	
	zip
	
2. 计算 1 这个页面的点击量
	
	再计算 "1-2" 跳转量,  必须要保证是同一个session跳转才算一次.   按照session分组
	
	
 */
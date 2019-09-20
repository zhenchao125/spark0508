import java.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

/**
  * Author lzc
  * Date 2019-09-20 08:58
  */
object SortBy {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("SortBy").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val rdd1: RDD[(String, Int)] = sc.parallelize(Array(("hello",2), ("hello", 3), ("world", 4), ("world", 1)))
        val rdd2 = rdd1.sortBy(x => x)(Ordering.Tuple2(Ordering.String.reverse, Ordering.Int.reverse), ClassTag(classOf[(String, Int)]))
        println(rdd2.collect().mkString(", "))
        
        sc.stop()
        
    }
}

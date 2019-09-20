package com.atguigu.sparkcore.day04.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-09-17 16:11
  */
object HBaseRead {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val hbaseConf: Configuration = HBaseConfiguration.create()
        hbaseConf.set("hbase.zookeeper.quorum", "hadoop201,hadoop202,hadoop203")
        hbaseConf.set(TableInputFormat.INPUT_TABLE, "student")
        
        
        val rdd1 = sc.newAPIHadoopRDD(
            hbaseConf,
            classOf[TableInputFormat],
            classOf[ImmutableBytesWritable],
            classOf[Result]
        )
        val rdd2 = rdd1.map{
            case (key, result) => {
//                Bytes.toString(result.getRow)
//                Bytes.toString(key.get())
                val cells= result.listCells()
                // 带入隐式转换, 内置了很多java和scala集合互转的方法
                import scala.collection.JavaConversions._
                for (cell <- cells) {
                    println(Bytes.toString(CellUtil.cloneQualifier(cell)))
                }
            }
        }
        
        rdd2.collect.foreach(println)
        sc.stop()
        
    }
}

/*
nosql  rowKey family 列名 值


 */
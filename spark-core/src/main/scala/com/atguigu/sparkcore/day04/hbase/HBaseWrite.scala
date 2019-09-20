package com.atguigu.sparkcore.day04.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-09-17 16:11
  */
object HBaseWrite {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val hbaseConf: Configuration = HBaseConfiguration.create()
        hbaseConf.set("hbase.zookeeper.quorum", "hadoop201,hadoop202,hadoop203")
        hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, "student")
        // 通过job来设置输出的格式的类
        val job = Job.getInstance(hbaseConf)
        job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
        job.setOutputKeyClass(classOf[ImmutableBytesWritable])
        job.setOutputValueClass(classOf[Put])
    
    
        val initialRDD = sc.parallelize(List(("10", "apple", "11"), ("20", "banana", "12"), ("30", "pear", "13")))
        val hbasaRDD = initialRDD.map {
            case (row, name, weight) => {
                // 封装rowKey
                val rowKey = new ImmutableBytesWritable()
                rowKey.set(Bytes.toBytes(row))
                
                val put = new Put(Bytes.toBytes(row))
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(name))
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("weight"), Bytes.toBytes(weight))
                (rowKey, put)
            }
        }
        hbasaRDD.saveAsNewAPIHadoopDataset(job.getConfiguration)
        sc.stop()
        
    }
}

/*
nosql  rowKey family 列名 值


 */
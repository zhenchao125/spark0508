package com.atguigu.spark.streaming.day01

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author lzc
  * Date 2019-09-23 10:24
  */
object CustumReceiver {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HighKafka")
        val ssc = new StreamingContext(conf, Seconds(3))
        
        ssc
            .receiverStream(new MyReceiver("hadoop201", 9999))
            .flatMap(_.split(" "))
            .map((_, 1))
            .reduceByKey(_ + _)
            .print(100)
        
        ssc.start()
        ssc.awaitTermination()
        
    }
}

/*
自定义数据源的本质就是自定义接收器

从Socket来接收数据
*/


class MyReceiver(val host: String, val port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {
    /*
    接收启动的时候调用的方法
    启动一个子线程, 循环不断的去接收数据
     */
    override def onStart(): Unit = {
        new Thread() {
            override def run(): Unit = receiveData()
        }.start()
        
    }
    
    /*
    接收器停止的时候回调方法
     */
    override def onStop(): Unit = ???
    
    
    // 接收数据
    def receiveData(): Unit = {
        // 从socket读数据
        try {
            val socket = new Socket(host, port)
            val reader = new BufferedReader(new InputStreamReader(socket.getInputStream, "utf-8"))
            
            var line = reader.readLine()
            while (line != null) {
                //
                store(line)
                line = reader.readLine()
            }
            reader.close()
            socket.close()
        } catch {
            case e: Exception => e.printStackTrace
        } finally {
            // 重启任务
            restart("重新连接")
        }
    }
}
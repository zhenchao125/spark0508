package com.atguigu.sparkcore.day04.jdbc

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019-09-17 15:30
  */
object JDBCWrite {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        //定义连接mysql的参数
        val driver = "com.mysql.jdbc.Driver"
        val url = "jdbc:mysql://hadoop201:3306/rdd"
        val userName = "root"
        val passWd = "aaa"
        
        val rdd2 = sc.parallelize(Array(3, 5, 8, 2, 1, 9, 5, 7))
        // 建立到mysql的连接
        
        val sql = "insert into user values(?)"
        /*rdd2.foreach(x => {
            Class.forName(driver)
            val conn: Connection = DriverManager.getConnection(url, userName, passWd)
            val ps: PreparedStatement = conn.prepareStatement(sql)
            ps.setInt(1, x)
            ps.execute()
            ps.close()
            conn.close()
            // 写
        })*/
        /*rdd2.foreachPartition(it => {
            Class.forName(driver)
            val conn: Connection = DriverManager.getConnection(url, userName, passWd)
            it.foreach(x => {
                val ps: PreparedStatement = conn.prepareStatement(sql)
                ps.setInt(1, x)
                ps.execute()
                ps.close()
            })
            conn.close()
        })*/
        rdd2.foreachPartition(it => {
            Class.forName(driver)
            val conn: Connection = DriverManager.getConnection(url, userName, passWd)
            val ps: PreparedStatement = conn.prepareStatement(sql)
            it.foreach(x => {
                ps.setInt(1, x)
                ps.addBatch()
            })
            ps.executeBatch()
            ps.close()
            conn.close()
        })
        sc.stop()
    }
}

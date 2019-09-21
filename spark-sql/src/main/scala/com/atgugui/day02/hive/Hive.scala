package com.atgugui.day02.hive

import org.apache.spark.sql.SparkSession

/**
  * Author lzc
  * Date 2019-09-21 11:12
  */
object Hive {
    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME", "atguigu")
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("Hive")
            .enableHiveSupport()  // 支持hive
            .config("spark.sql.warehouse.dir", "hdfs://hadoop201:9000/user/hive/warehouse")
            .getOrCreate()
        import spark.implicits._
        
//        spark.sql("show tables").show
//        spark.sql("select sum(sal) from emp").show()
        spark.sql("create database if not exists sql0509") //
        spark.sql("use sql0509")
        spark.sql("create table user1(id int, name string) row format DELIMITED FIELDS TERMINATED BY '\t'")
        spark.sql("insert into user1 values(1, 'lisi')")
//        spark.sql("show tables").show
        spark.sql("select * from user1").show
    }
}

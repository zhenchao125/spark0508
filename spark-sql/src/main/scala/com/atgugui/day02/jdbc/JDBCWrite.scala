package com.atgugui.day02.jdbc

import java.util.Properties

import com.atgugui.day01.User
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * Author lzc
  * Date 2019-09-21 09:09
  */
object JDBCWrite {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("JDBCRead")
            .getOrCreate()
        import spark.implicits._
        val arr1 = spark.sparkContext.parallelize(Seq(User("lisi", 20, "mael"), User("ww", 18, "mael")))
        val df: DataFrame = arr1.toDF
        
        // 通用的写
        /*df.write
            .format("jdbc")
            .option("url", "jdbc:mysql://hadoop201:3306/rdd")
            .option("user", "root")
            .option("password", "aaa")
            .option("dbtable", "person1")
            .mode(SaveMode.Append)
            .save()*/
        
        // 专用的写
        val props = new Properties()
        props.setProperty("user", "root")
        props.setProperty("password", "aaa")
        df.write
            .jdbc("jdbc:mysql://hadoop201:3306/rdd", "person2", props)
        
        spark.close()
    }
}

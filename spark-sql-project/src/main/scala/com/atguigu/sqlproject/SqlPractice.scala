package com.atguigu.sqlproject

import org.apache.spark.sql.SparkSession

/**
  * Author lzc
  * Date 2019-09-21 14:25
  */
object SqlPractice {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("SqlPractice")
            .enableHiveSupport()
            .getOrCreate()
        spark.sql("use sql_project")
        // 表之间的管理
        spark.sql(
            """
              |select
              |	c.*,
              |	u.click_product_id,
              |	p.product_name
              |from user_visit_action u join city_info c join product_info p
              |on u.city_id=c.city_id and u.click_product_id=p.product_id
              |where u.click_product_id>-1
            """.stripMargin).createOrReplaceTempView("t1")
        
        spark.sql(
            """
              |select
              |	t1.area,
              |	t1.product_name,
              |	count(*) click_count
              |from t1
              |group by area, product_name
            """.stripMargin).createOrReplaceTempView("t2")
        
        spark.sql(
            """
              |select
              |	*,
              |	rank() over(partition by area order by click_count desc) rank
              |from t2
            """.stripMargin).createOrReplaceTempView("t3")
        
        spark.sql(
            """
              |select
              |	area,
              |	product_name,
              |	click_count
              |from t3
              |where rank<=3
            """.stripMargin).show(100)
        spark.close()
    }
}

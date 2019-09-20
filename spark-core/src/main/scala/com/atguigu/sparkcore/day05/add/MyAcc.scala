package com.atguigu.sparkcore.day05.add

import org.apache.spark.util.AccumulatorV2

class MyAcc extends AccumulatorV2[Long, Long] {
    var sum = 0L  // 缓存中间值
    
    // 判断零值
    override def isZero: Boolean = sum == 0
    
    // 复制累加器
    override def copy(): AccumulatorV2[Long, Long] = {
        val newAcc = new MyAcc
        newAcc.sum = sum  // 当前缓存的值赋值给新的acc
        newAcc
    }
    
    // 重置累加器  把缓存的值重置为 "0"值
    override def reset(): Unit = sum = 0
    
    // 核心功能: 累加
    override def add(v: Long): Unit = sum += v
    
    // 合并: 合并累加器
    override def merge(other: AccumulatorV2[Long, Long]): Unit = {
        println("aaaa")
        /*val o: MyAcc = other.asInstanceOf[MyAcc]
        this.sum += o.sum*/
        
        other match {
            case o: MyAcc => this.sum += o.sum
            case _ => throw new IllegalStateException
        }
    }
    
    // 返回最终的累加后的值
    override def value: Long = this.sum
}

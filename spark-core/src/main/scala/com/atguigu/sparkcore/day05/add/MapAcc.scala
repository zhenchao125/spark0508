package com.atguigu.sparkcore.day05.add

import org.apache.spark.util.AccumulatorV2

class MapAcc extends AccumulatorV2[Long, Map[String, Double]] {
    var map: Map[String, Double] = Map[String, Double]()
    var count = 0L // 记录参与累加的元素的个数
    
    override def isZero: Boolean = map.isEmpty && count == 0
    
    override def copy(): AccumulatorV2[Long, Map[String, Double]] = {
        val acc: MapAcc = new MapAcc
        acc.map = map
        acc.count = count
        acc
    }
    
    override def reset(): Unit = {
        map = Map[String, Double]()
        count = 0
    }
    
    override def add(v: Long): Unit = {
        // sum, max, min
        count += 1
        map += "sum" -> (map.getOrElse("sum", 0D) + v)
        map += "max" -> map.getOrElse("max", Long.MinValue.toDouble).max(v)
        map += "min" -> map.getOrElse("min", Long.MaxValue.toDouble).min(v)
    }
    
    override def merge(other: AccumulatorV2[Long, Map[String, Double]]): Unit = {
        other match {
            case o: MapAcc =>
                this.count += o.count
                this.map += "sum" -> (this.map.getOrElse("sum", 0D) + o.map.getOrElse("sum", 0D))
                this.map += "max" -> this.map.getOrElse("max", Long.MinValue.toDouble).max(o.map.getOrElse("max", Long.MinValue.toDouble))
                this.map += "min" -> this.map.getOrElse("min", Long.MaxValue.toDouble).min(o.map.getOrElse("min", Long.MaxValue.toDouble))

            case _ => throw new UnsupportedOperationException
        }
    }
    
    override def value: Map[String, Double] = {
        this.map += "avg" -> this.map.getOrElse("sum", 0D) / this.count
        this.map
    }
}

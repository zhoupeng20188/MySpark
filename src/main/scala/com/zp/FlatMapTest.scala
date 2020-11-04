package com.zp

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author zp
  * @create 2020/11/4 9:52
  */
object FlatMapTest {
  def main(args: Array[String]): Unit = {
    // 设置运行环境
    val config = new SparkConf().setMaster("local[*]")
      .setAppName("wordCount")
    val sc = new SparkContext(config)
    val unit: RDD[List[Int]] = sc.makeRDD(Array(List(1,2),List(3,4)))
    val unit2: RDD[Int] = unit.flatMap(datas=>datas)
    unit2.foreach(println)
  }

}

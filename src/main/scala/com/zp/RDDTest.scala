package com.zp

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author zp
  * @create 2020/11/3 14:44
  */
object RDDTest {
  def main(args: Array[String]): Unit = {
    // 设置运行环境
    val config = new SparkConf().setMaster("local[*]")
      .setAppName("wordCount")
    val sc = new SparkContext(config)
    println(sc)
    // 读取文件，按行读取
//    val lines: RDD[String] = sc.textFile("input",2)
    val value: RDD[Int] = sc.makeRDD(Array(1,2,3,4,5),4)
    // 保存到文件
    value.saveAsTextFile("output")
  }
}

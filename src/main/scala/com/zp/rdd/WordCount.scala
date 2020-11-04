package com.zp.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author zp
  * @create 2020/11/3 14:44
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    // 设置运行环境
    val config = new SparkConf().setMaster("local[*]")
      .setAppName("wordCount")
    val sc = new SparkContext(config)
    println(sc)
    // 读取文件，按行读取
    val lines: RDD[String] = sc.textFile("input")
    // 按行将数据分解为一个个的单词
    val words: RDD[String] = lines.flatMap(_.split(" "))
    // 数据类型转换
    val wordToOne: RDD[(String, Int)] = words.map((_,1))
    // 分组聚合
    val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_+_)
    val result: Array[(String, Int)] = wordToSum.collect()
    result.foreach(println)
  }
}

package com.zp.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author zp
  * @create 2020/11/4 10:14
  */
object SampleTest {
  def main(args: Array[String]): Unit = {
    // 设置运行环境
    val config = new SparkConf().setMaster("local[*]")
      .setAppName("wordCount")
    val sc = new SparkContext(config)
    val unit: RDD[Int] = sc.makeRDD(1 to 10)
    // 第一个参数表示每个数取出后是否放回，true表示放回
    // 第二个参数为采样的个数，1为全部抽出，0为全部不抽出
    // 第三个参数为随机种子
    val sample: RDD[Int] = unit.sample(false, 0.4,1)
    sample.foreach(println)
  }

}

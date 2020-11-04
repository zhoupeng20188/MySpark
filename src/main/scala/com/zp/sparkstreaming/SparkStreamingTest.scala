package com.zp.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingTest {
  def main(args: Array[String]): Unit = {
    // 设置运行环境
    val config = new SparkConf().setMaster("local[*]")
      .setAppName("wordCount")

    // 第二个参数为采集周期
    val context = new StreamingContext(config, Seconds(3))
    // 在指定的端口中采集数据
    val socketStream: ReceiverInputDStream[String] = context.socketTextStream("192.168.223.129",9999)
    // 扁平化
    val wordStream: DStream[String] = socketStream.flatMap(x=>x.split(" "))
    // 转换数据格式
    val mapStream: DStream[(String, Int)] = wordStream.map((_,1))
    // 聚合
    val wordToSum: DStream[(String, Int)] = mapStream.reduceByKey(_+_)
    wordToSum.print()

    // 启动采集器
    context.start();
    // 等待采集器执行
    context.awaitTermination();

  }

}

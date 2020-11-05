package com.zp.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * 滑动窗口
  * @Author zp
  * @create 2020/11/5 17:08
  */
object WindowTest {
  def main(args: Array[String]): Unit = {
    // 设置运行环境
    val config = new SparkConf().setMaster("local[*]")
      .setAppName("wordCount")

    // 第二个参数为采集周期
    val context = new StreamingContext(config, Seconds(5))

    // 保存数据的状态，需要设定checkpoint的路径
    context.checkpoint("checkpoint");


    // 从kafka中消费消息

    val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
      context,
      "192.168.129.148:2181",
      "groupTest",
      Map("topic001" -> 4))

    // 第一个参数为窗口大小，第二个参数为滑动的步长
    // 窗口大小和步长都为采集周期的整数倍
    val windowDStream: DStream[(String, String)] = kafkaDStream.window(Seconds(10),Seconds(5))

    // 扁平化
    val wordStream: DStream[String] = windowDStream.flatMap(t => t._2.split(" "))
    // 转换数据格式
    val mapStream: DStream[(String, Int)] = wordStream.map((_, 1))

    // 将结果进行聚合
    val stateDStream: DStream[(String, Int)] = mapStream.updateStateByKey {
      case (seq, buffer) => {
        val sum: Int = buffer.getOrElse(0) + seq.sum
        Option(sum)
      }
    }
    stateDStream.print()

    // 启动采集器
    context.start();
    // 等待采集器执行
    context.awaitTermination();

  }

}

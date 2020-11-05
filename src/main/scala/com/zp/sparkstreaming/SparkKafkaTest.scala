package com.zp.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 从kafka中读取消息
  * 默认数据统计不会累加
  * @Author zp
  * @create 2020/11/5 9:03
  */
object SparkKafkaTest {
  def main(args: Array[String]): Unit = {
    // 设置运行环境
    val config = new SparkConf().setMaster("local[*]")
      .setAppName("wordCount")

    // 第二个参数为采集周期
    val context = new StreamingContext(config, Seconds(5))
    // 从kafka中消费消息

    val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
      context,
      "192.168.129.148:2181",
      "groupTest",
      Map("topic001" -> 4))

    // 指定kafka的broker地址
    //    val brokerList = "192.168.129.148:32781,192.168.129.148:32782,192.168.129.148:32783"
    //    val kafkaParams: Map[String, String] = Map(
    //      // 这个参数是producer端用的，consumer端无法设置brokerList
    //      //      "metadata.broker.list" -> brokerList,
    //      "zookeeper.connect" -> "192.168.129.148:2181",
    //      "zookeeper.connection.timeout.ms" -> "40000",
    //      "group.id" -> "groupTest"
    //    )
    // 如果要带上kafka的参数，使用createStream函数时必须带上函数的参数[String,
    //      String, StringDecoder, StringDecoder]
    //    val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream[String,
    //      String, StringDecoder, StringDecoder](
    //      context,
    //      kafkaParams,
    //      Map("topic001" -> 4),
    //      StorageLevel.MEMORY_AND_DISK)
    // 扁平化
    val wordStream: DStream[String] = kafkaDStream.flatMap(t => t._2.split(" "))
    // 转换数据格式
    val mapStream: DStream[(String, Int)] = wordStream.map((_, 1))
    // 聚合
    val wordToSum: DStream[(String, Int)] = mapStream.reduceByKey(_ + _)
    wordToSum.print()

    // 启动采集器
    context.start();
    // 等待采集器执行
    context.awaitTermination();

  }
}

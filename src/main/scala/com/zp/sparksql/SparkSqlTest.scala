package com.zp.sparksql

import org.apache.spark.{SparkConf, sql}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * @Author zp
  * @create 2020/11/4 15:48
  */
object SparkSqlTest {
  def main(args: Array[String]): Unit = {
    // 设置运行环境
    val config = new SparkConf().setMaster("local[*]")
      .setAppName("wordCount")

    // 创建sparkSession对象
    val session: SparkSession = SparkSession.builder().config(config).getOrCreate()
    // 从json文件中读取并得到dataFrame
    val df: DataFrame = session.read.json("input/user.json")
    df.show()

    df.createTempView("t1")
    session.sql("select name from t1").show()

    // df与ds之间的转换需要引入包
    import session.implicits._
    val ds: Dataset[User] = df.as[User]
    ds.select("age").show()
    session.stop()
  }

}
case class User(name:String,age:Long)

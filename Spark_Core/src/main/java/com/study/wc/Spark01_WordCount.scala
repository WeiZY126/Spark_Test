package com.study.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark01_WordCount {
  def main(args: Array[String]): Unit = {
    //Application
    //Spark框架
    //TODO 建立和spark框架的链接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    //TODO 执行业务操作
    //1.读取文件，获取一行一行数据
    val lines: RDD[String] = sc.textFile("datas")

    //2.将一行数据拆分，形成一个一个单词（分词）
    //扁平化：整体拆分成个体
    //hello world => hello,world
    val words: RDD[String] = lines.flatMap(_.split(" "))

    //3.将数据根据单词进行分组，便于统计
    //(hello,hello,hello)(world,world)
    val value: RDD[(String, Iterable[String])] = words.groupBy(word => word)

    //4.对分组后的数据进行转换
    //(hello,3),(world,2)
    val wordToCount = value.map {
      case (word, list) => {
        (word, list.size)
      }
    }

    //5.将转换结果采集到控制台
    println(wordToCount)
    //TODO 关闭链接
    sc.stop()
  }
}

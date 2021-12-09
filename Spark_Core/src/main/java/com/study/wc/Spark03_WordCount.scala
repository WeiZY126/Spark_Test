package com.study.wc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark03_WordCount {
  def main(args: Array[String]): Unit = {
    //spark框架提供更多功能，可以将分组和聚合使用一个方法实现
    //Spark框架
    //TODO 建立和spark框架的链接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    //TODO 执行业务操作
    val lines: RDD[String] = sc.textFile("datas/*")

    val words: RDD[String] = lines.flatMap(_.split(" "))

    val wordToOne: RDD[(String, Int)] = words.map(word => (word, 1))

    //reduceByKey:相同的key可以对value进行reduce聚合
    wordToOne.reduceByKey(_+_)
  }

}

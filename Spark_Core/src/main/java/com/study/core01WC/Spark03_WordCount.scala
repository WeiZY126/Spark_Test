package com.study.core01WC

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark03_WordCount {
  def main(args: Array[String]): Unit = {
    //Application
    //Spark框架
    //TODO 建立和spark框架的链接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    //TODO 执行业务操作
    val lines: RDD[String] = sc.textFile("datas/*")

    val words: RDD[String] = lines.flatMap(_.split(" "))

    val wordToOne: RDD[(String, Int)] = words.map(word => (word, 1))
    //reduceByKey:相同key的数据，可以对value进行reduce聚合
    val wordToCount: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)


    val array: Array[(String, Int)] = wordToCount.collect()
    array.foreach(println)
    //TODO 关闭链接
    sc.stop()
  }

}

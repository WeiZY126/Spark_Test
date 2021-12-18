package com.study.core03RDD.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark24_RDD_Test {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    //TODO 每个省份每个广告的点击量前三
    //格式：时间戳 省份 城市 用户 广告
    val logs: RDD[String] = sc.textFile("datas/agent.log")
    //根据空格拆分出省份，广告
    val mapRDD: RDD[((String, String), Int)] = logs.map(str => {
      val strings: Array[String] = str.split(" ")
      ((strings(1), strings(4)), 1)
    })
    val reduceRDD: RDD[((String, String), Int)] = mapRDD.reduceByKey(_ + _)
    //    val groupRDD: RDD[(String, Iterable[(String, Int)])] = reduceRDD.map((tup) => (tup._1._1, (tup._1._2, tup._2))).groupByKey()
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = reduceRDD.map {
      case ((pre, ad), sum) => (pre, (ad, sum))
    }.groupByKey()
    val value: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(iter => iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3))
    value.collect().foreach(println)
    //TODO 关闭环境
    sc.stop()
  }

}

package com.study.core03RDD.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark16_RDD_groupByKey {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    //TODO 算子-Key-Value类型

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("b", 4)), 2)
    //将数据源中的数据，相同key的数据分在一个组中，形成一个对偶元组
    //元组中的第一个元素就是key
    //元组中的第二个元素就是相同key的value的集合
    val groupRDD: RDD[(String, Iterable[Int])] = rdd.groupByKey()
    //value不会独立出来
    val groupRDD1: RDD[(String, Iterable[(String, Int)])] = rdd.groupBy(_._1)

    groupRDD.collect().foreach(println)
    //TODO 关闭环境
    sc.stop()
  }

}

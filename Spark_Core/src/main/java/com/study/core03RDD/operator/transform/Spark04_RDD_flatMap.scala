package com.study.core03RDD.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark04_RDD_flatMap {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    //TODO 取出每个分区的最大值
    val rdd: RDD[List[Int]] = sc.makeRDD(List(List(1, 2), List(3, 4)))
    val flatRDD: RDD[Int] = rdd.flatMap(list => {
      list
    })
    flatRDD.collect().foreach(println)
    //TODO 关闭环境
    sc.stop()
  }

}

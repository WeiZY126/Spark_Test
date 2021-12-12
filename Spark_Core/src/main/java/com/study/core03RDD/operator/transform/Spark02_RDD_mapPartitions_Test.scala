package com.study.core03RDD.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_mapPartitions_Test {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    //TODO 取出每个分区的最大值
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    val mpRDD: RDD[Int] = rdd.mapPartitions(iter => {
      List(iter.max).iterator
    })
    mpRDD.collect().foreach(println)
    //TODO 关闭环境
    sc.stop()
  }

}

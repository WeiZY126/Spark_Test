package com.study.core03RDD.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


object Spark03_RDD_mapPartationsWithIndex_test1 {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    //TODO 取出每个分区的最大值
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    val mpwiRDD: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        iter.map(num => (index, num))
      })
    mpwiRDD.collect().foreach(println)
    //TODO 关闭环境
    sc.stop()
  }

}

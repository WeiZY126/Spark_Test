package com.study.core03RDD.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_mapPartationsWithIndex {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    //TODO 取出每个分区的最大值
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    val mpwiRDD: RDD[Int] = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        if (index != 1)
          Nil.iterator
        else
          iter
      })
    mpwiRDD.collect().foreach(println)
    //TODO 关闭环境
    sc.stop()
  }

}

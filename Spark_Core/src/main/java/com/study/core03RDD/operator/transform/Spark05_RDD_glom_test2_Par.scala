package com.study.core03RDD.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark05_RDD_glom_test2_Par {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    //TODO 分区内取最大值，分区间最大值求和
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    rdd.saveAsTextFile("outputBefore")
    val mapRDD: RDD[Int] = rdd.map(_ * 2)
    mapRDD.saveAsTextFile("outputAfter")
    //TODO 关闭环境
    sc.stop()
  }

}

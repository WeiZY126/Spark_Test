package com.study.core03RDD.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark07_RDD_filter_test1 {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    //TODO 取出2015年5月17日路径
    val rdd: RDD[String] = sc.textFile("datas/apache.log")
    val filterRDD: RDD[String] = rdd.filter(_.split(" ")(3).startsWith("17/05/2015"))

    filterRDD.collect().foreach(println)
    //TODO 关闭环境
    sc.stop()
  }

}

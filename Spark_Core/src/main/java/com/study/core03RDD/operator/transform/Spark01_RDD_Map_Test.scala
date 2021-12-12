package com.study.core03RDD.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Map_Test {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[String] = sc.textFile("datas/apache.log")

    //长字符串
    val mapRdd: RDD[String] = rdd.map(_.split(" ")(6))
    //短字符串

    //将处理的数据保存成分区文件
    mapRdd.collect().foreach(println)
    //TODO 关闭环境
    sc.stop()
  }

}

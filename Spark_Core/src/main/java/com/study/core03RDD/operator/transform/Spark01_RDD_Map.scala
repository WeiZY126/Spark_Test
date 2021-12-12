package com.study.core03RDD.operator.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark01_RDD_Map {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    //1,2,3,4
    //2,4,6,8
    def mapFunction(num: Int): Int = {
      num * 2
    }

    val mapRdd: RDD[Int] = rdd.map(_ * 2)
    //将处理的数据保存成分区文件
    mapRdd.collect().foreach(println)
    //TODO 关闭环境
    sc.stop()
  }

}

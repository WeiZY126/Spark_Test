package com.study.core03RDD.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark05_RDD_glom {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    //TODO 算子-glom
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    //list=>int
    //Int=>Array
    val glomRDD: RDD[Array[Int]] = rdd.glom()
    glomRDD.collect().foreach(data => println(data.mkString(",")))
    //TODO 关闭环境
    sc.stop()
  }

}

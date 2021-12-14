package com.study.core03RDD.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark13_RDD_twoValue_test1 {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    //TODO 算子-sortBy

    //两个数据源要求分区数量保持一致
    //两个数据源要求分区内数据数量保持一致
    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)
    val rdd2: RDD[Int] = sc.makeRDD(List(3, 4, 5, 6),2)

    //拉链
    val rdd6: RDD[(Int, Int)] = rdd1.zip(rdd2)
    rdd6.collect().foreach(println)
    //TODO 关闭环境
    sc.stop()
  }

}

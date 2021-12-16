package com.study.core03RDD.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark18_RDD_foldByKey {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    //TODO 算子-Key-Value类型

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3), ("b", 4), ("b", 5), ("a", 6)), 2)

    //如果聚合计算时，分区内和分区间计算规则相同，spark提供了简化的方法
    val foldRDD: RDD[(String, Int)] = rdd.foldByKey(0)(_ + _)
    foldRDD.collect().foreach(println)
    //TODO 关闭环境
    sc.stop()
  }

}

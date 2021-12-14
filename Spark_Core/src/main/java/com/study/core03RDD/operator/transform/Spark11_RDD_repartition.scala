package com.study.core03RDD.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark11_RDD_repartition {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    //TODO 算子-repartition
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)

    //coalesce算子可以扩大分区，但是如果不进行shuffle，不起作用
//    val newRDD: RDD[Int] = rdd.coalesce(3, false)

    //扩大分区：repartition，底层调用的就是带shuffle的coalesce
    val newRDD: RDD[Int] = rdd.repartition(3)
    newRDD.saveAsTextFile("output")

    //TODO 关闭环境
    sc.stop()
  }

}

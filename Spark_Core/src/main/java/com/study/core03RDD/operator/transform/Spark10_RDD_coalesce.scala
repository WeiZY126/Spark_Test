package com.study.core03RDD.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark10_RDD_coalesce {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    //TODO 算子-coalesce
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 3)

    //coalesce默认不会打乱数据重新组合
    //这种情况下缩减分区可能会导致数据不均衡，出现数据倾斜
    //如果想让数据均衡，可以进行shuffle处理
    val newRDD: RDD[Int] = rdd.coalesce(2,true)
    newRDD.saveAsTextFile("output")

    //TODO 关闭环境
    sc.stop()
  }

}

package com.study.core03RDD.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Aggregate {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)

    //TODO-行动算子

    //aggregateByKey初始值只会参加分区内计算
    //aggregate:初始值会参与分区内计算，并且会参与分区间计算
//    val result: Int = rdd.aggregate(10)(_ + _, _ + _)

    //fold
    val result: Int = rdd.fold(10)(_ + _)
    println(result)
    sc.stop()
  }

}

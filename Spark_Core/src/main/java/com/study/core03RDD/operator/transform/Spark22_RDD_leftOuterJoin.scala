package com.study.core03RDD.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark22_RDD_leftOuterJoin {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    //TODO 算子-Key-Value类型

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 4), ("b", 5)))
    val leftJoinRDD: RDD[(String, (Int, Option[Int]))] = rdd1.leftOuterJoin(rdd2)
    val rightJoinRDD: RDD[(String, (Option[Int], Int))] = rdd2.rightOuterJoin(rdd1)
//    leftJoinRDD.collect().foreach(println)
    rightJoinRDD.collect().foreach(println)



    //TODO 关闭环境
    sc.stop()
  }

}

package com.study.core03RDD.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark17_RDD_aggregateByKey {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    //TODO 算子-Key-Value类型

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3), ("b", 4), ("b", 5), ("a", 6)), 2)

    //(a,[1,2])(a,[3,4])=>(a,[6])
    //aggregateByKey存在函数的柯里化，有两个参数列表
    //第一个参数列表，需要传递一个参数：初始值
    //    主要用于当碰见第一个key的时候，和value进行分区内计算
    //第二个参数列表需要传入两个参数
    //    第一个参数表示分区内计算规则
    //    第二个参数表示分区间计算规则
    val aggRDD: RDD[(String, Int)] = rdd.aggregateByKey(0)((x, y) => {
      math.max(x, y)
    }, (x, y) => {
      x + y
    })
    aggRDD.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }

}

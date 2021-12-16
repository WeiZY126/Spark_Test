package com.study.core03RDD.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark19_RDD_combineByKey {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    //TODO 算子-Key-Value类型

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3), ("b", 4), ("b", 5), ("a", 6)), 2)

    //combineByKey:方法需要三个参数
    //第一个参数表示：将相同key的第一个数据进行结构转换，实现操作
    //第一二参数表示：分区内的计算
    //第三个参数表示：分区间的计算
    val combineRDD: RDD[(String, (Int, Int))] = rdd.combineByKey(v => (v, 1), (x: (Int, Int), y) => {
      (x._1 + y, x._2 + 1)
    }, (x: (Int, Int), y: (Int, Int)) => {
      (x._1 + y._1, x._2 + y._2)
    })
    val result: RDD[(String, Int)] = combineRDD.mapValues {
      case (num, count) => num / count
    }
    result.collect().foreach(println)
    //TODO 关闭环境
    sc.stop()
  }

}

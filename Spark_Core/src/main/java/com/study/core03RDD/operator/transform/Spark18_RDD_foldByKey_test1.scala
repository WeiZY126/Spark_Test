package com.study.core03RDD.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark18_RDD_foldByKey_test1 {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    //TODO 算子-Key-Value类型

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3), ("b", 4), ("b", 5), ("a", 6)), 2)

    //aggregateByKey最终返回类型应该和初始值一致
    //    val aggRDD: RDD[(String, String)] = rdd.aggregateByKey("")(_ + _)
    //获取相同key的数据的平均值=>(a,3),(b,4)
    val aggRDD: RDD[(String, (Int, Int))] = rdd.aggregateByKey((0, 0))((x, y) => {
      (x._1 + y, x._2 + 1)
    }, (x, y) => {
      (x._1 + y._1, x._2 + y._2)
    })
    val result: RDD[(String, Int)] = aggRDD.mapValues {
      case (num, count) => num / count
    }
    result.collect().foreach(println)
    //TODO 关闭环境
    sc.stop()
  }

}

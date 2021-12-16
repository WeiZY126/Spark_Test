package com.study.core03RDD.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark21_RDD_join {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    //TODO 算子-Key-Value类型

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 4), ("a", 5), ("c", 6)))

    //join:两个不同数据源的数据，相同key的valuehi连接在一起，形成元组
    //如果两个数据源中key没有匹配上，那么数据不会出现在结果中
//  //如果两个数据源中key有多个相同的，会依次匹配，可能出现笛卡尔积，数据量会几何性增长
    val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
    joinRDD.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }

}

package com.study.core03RDD.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark08_RDD_sample {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    //TODO 算子-sample
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

    //sample算子需要传递三个参数
    //1.第一个参数表示，抽取后是否将数据放回 true(放回) false(丢弃)
    //2.第二个参数表示，数据源中每条数据被抽取的概率
    //如果抽取不放回：数据源中每条数据被抽取的概率，基准值
    //如果抽取放回：表示数据源中每条数据被抽取的可能次数
    //3.第三个参数表示，抽取数据时随机算法的种子
    //如果不传递第三个参数，那么使用当前系统时间
//    val sampleRDD: RDD[Int] = rdd.sample(false, 0.4)
    val sampleRDD: RDD[Int] = rdd.sample(true, 5)
    println(sampleRDD.collect().mkString(","))

    //TODO 关闭环境
    sc.stop()
  }

}

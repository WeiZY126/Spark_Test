package com.study.core03RDD.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark06_RDD_groupBy_test1 {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    //TODO 算子-groupBy
    val rdd: RDD[String] = sc.makeRDD(List("hello","spark","scala","hadoop"), 2)

    //分组和分区没有必然关系
    val groupRDD: RDD[(Char, Iterable[String])] = rdd.groupBy(_.charAt(0))
    groupRDD.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }

}

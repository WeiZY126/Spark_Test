package com.study.core03RDD.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark20_RDD_different {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    //TODO 算子-Key-Value类型

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3), ("b", 4), ("b", 5), ("a", 6)), 2)

    /*
    reduceByKey:
    combineByKeyWithClassTag[V]
    ((v: V) => v,   //第一个值不会参与计算
    func,           //分区内计算规则
    func)           //分区间计算规则

    aggregateByKey:
    combineByKeyWithClassTag[U](
    (v: V) => cleanedSeqOp(createZero(), v),//初始值和第一个key的value进行的分区内计算规则
    cleanedSeqOp,     //分区内计算规则
    combOp)           //分区间计算规则

    foldByKey:
    combineByKeyWithClassTag[V](
    (v: V) => cleanedFunc(createZero(), v),   //初始值和第一个key的value进行的分区内计算规则
    cleanedFunc,    //分区内计算规则
    cleanedFunc)    //分区间计算规则

    combineByKey:
    combineByKeyWithClassTag(
    createCombiner,   //相同key第一条数据进行的处理
    mergeValue,       //分区内数据的处理函数
    mergeCombiners,   //分区间数据的处理函数
    mapSideCombine,
    serializer)(null)
     */
    rdd.reduceByKey(_ + _)
    rdd.aggregateByKey(0)(_ + _, _ + _)
    rdd.foldByKey(0)(_ + _)
    rdd.combineByKey(v => v, (x: Int, y) => x + y, (x: Int, y: Int) => x + y)
    //TODO 关闭环境
    sc.stop()
  }

}

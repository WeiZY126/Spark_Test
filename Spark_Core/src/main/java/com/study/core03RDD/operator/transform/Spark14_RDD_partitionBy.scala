package com.study.core03RDD.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}


object Spark14_RDD_partitionBy {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    //TODO 算子-Key-Value类型

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    val mapRDD: RDD[(Int, Int)] = rdd.map((_, 1))

    //RDD=>PairRDDFunctions
    //隐式转换（二次编译）
    val newRDD: RDD[(Int, Int)] = mapRDD.partitionBy(new HashPartitioner(2))
    //如果分区器数量和类型一样，则直接返回自己
    newRDD.partitionBy(new HashPartitioner(2)).saveAsTextFile("output")
    //TODO 关闭环境
    sc.stop()
  }

}

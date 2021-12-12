package com.study.core03RDD.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Map_Par {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)


    //1.rdd的计算一个分区内的数据是串行执行逻辑
    //只有前面一个数据全部逻辑执行完毕后才会执行下一个数据
    //分区内数据执行是有序的
    //2.不同分区的数据计算是无序的
    val mapRdd: RDD[Int] = rdd.map(num => {
      println(">>>>" + num)
      num
    })

    val mapRdd1: RDD[Int] = mapRdd.map(num => {
      println("#####" + num)
      num
    })
    mapRdd1.collect()
    //TODO 关闭环境
    sc.stop()
  }

}

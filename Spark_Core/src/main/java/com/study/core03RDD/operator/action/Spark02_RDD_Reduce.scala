package com.study.core03RDD.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Reduce {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    //TODO-行动算子

    //reduce
    val i: Int = rdd.reduce(_ + _)
    println(i)

    //collect:方法会将不同分区的数据按照分区的顺序采集到driver端内存中进行处理，形成数组
    val ints: Array[Int] = rdd.collect()
    println(ints.mkString(","))

    //count:RDD数据源中数据的个数
    val cnt: Long = rdd.count()
    println(cnt)

    //first:获取数据源中的第一个
    val first: Int = rdd.first()
    println(first)

    //take:取前几个
    val take: Array[Int] = rdd.take(2)
    println(take.mkString(","))

    //takeOrdered:先排序，后取n个
    val rdd1: RDD[Int] = sc.makeRDD(List(4,2,3,1))
    val takeOr: Array[Int] = rdd.takeOrdered(3)
    println(takeOr.mkString(","))
    sc.stop()
  }

}

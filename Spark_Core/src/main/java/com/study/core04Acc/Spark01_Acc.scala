package com.study.core04Acc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Acc {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    //    //包含分区内的计算和分区间的计算
    //    val i: Int = rdd.reduce(_ + _)
    //    println(i)
    var sum = 0
    rdd.foreach(num => {
      sum += num
    })
    println(sum)
    sc.stop()
  }
}

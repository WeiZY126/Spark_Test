package com.study.core04Acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Acc {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    //获取系统的累加器
    //spark默认提供了简单数据聚合的累加器
    val sumAcc: LongAccumulator = sc.longAccumulator("sum")

    val mapRDD: RDD[Int] = rdd.map(num => {
      //使用累加器
      sumAcc.add(num)
      num
    })
    //获取累加器的值
    //少加：map是转换算子，如果没有行动算子，那么他不会执行
    println(sumAcc.value)

    //多加：行动算子多
    //一般情况下，累加器放在行动算子中操作
    mapRDD.collect()
    mapRDD.collect()
    println(sumAcc.value)
    sc.stop()
  }
}

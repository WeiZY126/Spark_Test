package com.study.core03RDD.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Save {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

//    val rdd: RDD[Int] = sc.makeRDD(List(1, 1, 1, 4),2)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List
    (("a", 1), ("a", 2), ("a", 3)),3)
    //TODO-行动算子

    rdd.saveAsTextFile("output")
    rdd.saveAsObjectFile("output1")
    //saveAsSequenceFile方法要求数据格式必须为key-value模式
    rdd.saveAsSequenceFile("output2")
    sc.stop()
  }

}

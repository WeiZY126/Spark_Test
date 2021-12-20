package com.study.core03RDD.IO

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_IO_Load {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.textFile("output")
    rdd.collect().foreach(println)

    val rdd1: RDD[(String, Int)] = sc.objectFile[(String, Int)]("output2")
    rdd1.collect().foreach(println)

    val rdd2: RDD[(String, Int)] = sc.sequenceFile[String, Int]("output3")
    rdd2.collect().foreach(println)
    sc.stop()
  }
}

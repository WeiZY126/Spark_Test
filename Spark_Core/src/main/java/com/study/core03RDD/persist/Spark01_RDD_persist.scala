package com.study.core03RDD.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_persist {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala","Hello Spark"))
    //TODO-行动算子
    val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))

    val mapRDD: RDD[(String, Int)] = flatRDD.map((_, 1))

    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    reduceRDD.collect().foreach(println)

    val rdd1: RDD[String] = sc.makeRDD(List("Hello Scala","Hello Spark"))
    //TODO-行动算子
    val flatRDD1: RDD[String] = rdd.flatMap(_.split(" "))

    val mapRDD1: RDD[(String, Int)] = flatRDD.map((_, 1))

    val groupRDD: RDD[(String, Iterable[Int])] = mapRDD1.groupByKey()

    groupRDD.collect().foreach(println)
    sc.stop()
  }
}

package com.study.core03RDD.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark13_RDD_twoValue {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    //TODO 算子-sortBy
    //交集，并集，差集要求两个数据源数据类型保持一致

    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val rdd2: RDD[Int] = sc.makeRDD(List(3, 4, 5, 6))
    val rdd7: RDD[String] = sc.makeRDD(List("3","4","5","6"))

    //交集 3 4
    val rdd3: RDD[Int] = rdd1.intersection(rdd2)
    rdd3.collect().foreach(println)
    //并集 1 2 3 4 5 6
    val rdd4: RDD[Int] = rdd1.union(rdd2)
    rdd4.collect().foreach(println)

    //差集:[1 2]
    val rdd5: RDD[Int] = rdd1.subtract(rdd2)

    //拉链[1-3 2-4 3-5 4-6]
    val rdd6: RDD[(Int, Int)] = rdd1.zip(rdd2)
    rdd6.collect().foreach(println)
    rdd1.zip(rdd7).collect().foreach(println)
    //TODO 关闭环境
    sc.stop()
  }

}

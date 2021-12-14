package com.study.core03RDD.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}


object Spark15_RDD_reduceByKey {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    //TODO 算子-Key-Value类型

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("b", 4)), 2)

    //reduceByKey:相同的key数据进行value数据的聚合
    //scala语言中一般的聚合操作都是两两聚合，spark也是两两聚合
    //[1,2,3]
    //[3,3]
    //[6]
    //如果key的数据只有一个，不会参加运算
    val reduce: RDD[(String, Int)] = rdd.reduceByKey((x, y) => {
      println(s"x=4x,y=$y")
      x + y
    })
    reduce.collect().foreach(println)

    //reduceByKey支持分区内预聚合功能，可以有效减少shuffle时落盘和读取的性能
    //TODO 关闭环境
    sc.stop()
  }

}

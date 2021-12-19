package com.study.core03RDD.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_persist {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    //TODO-行动算子
    val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))

    val mapRDD: RDD[(String, Int)] = flatRDD.map(word => {
      println("@")
      (word, 1)
    })

    //cache默认持久化的操作，只能将数据保存到内存中，如果想要使用磁盘，需使用persist
    mapRDD.cache()

    mapRDD.persist(StorageLevel.DISK_ONLY)

    //持久化操作必须在行动算子执行时完成的

    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    reduceRDD.collect().foreach(println)

    val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()

    groupRDD.collect().foreach(println)
    sc.stop()
  }
}

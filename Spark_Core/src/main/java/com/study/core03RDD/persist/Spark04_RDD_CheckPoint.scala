package com.study.core03RDD.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_CheckPoint {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    sc.setCheckpointDir("CheckPoint/")

    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    //TODO-行动算子
    val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))

    val mapRDD: RDD[(String, Int)] = flatRDD.map(word => {
      println("@")
      (word, 1)
    })

    //checkpoint需要落盘，需要指定检查点保存路径
    //检查点路径中保存的文件，当作业执行完毕后，不会被删除
    //一般保存在分布式文件存储系统中
    mapRDD.checkpoint()

    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    reduceRDD.collect().foreach(println)

    val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()

    groupRDD.collect().foreach(println)
    sc.stop()
  }
}

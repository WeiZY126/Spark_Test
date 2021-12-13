package com.study.core03RDD.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat
import java.util.Date


object Spark06_RDD_groupBy_test2 {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    //TODO 获取每个时间段访问量
    val rdd: RDD[String] = sc.textFile("datas/apache.log")
//    val groupRDD: RDD[(String, Iterable[String])] = rdd.groupBy(_.split(" ")(3).split(":")(1))
//    val result: RDD[(String, Int)] = groupRDD.map(tuple => (tuple._1, tuple._2.size))
//    result.collect().foreach(println)

    val timeRDD: RDD[(String, Iterable[(String, Int)])] = rdd.map(line => {
      val time = line.split(" ")(3)
      val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
      val date: Date = sdf.parse(time)
      val sdf1 = new SimpleDateFormat("HH")
      val hour: String = sdf1.format(date)
      (hour, 1)
    }).groupBy(_._1)

    timeRDD.map {
      case (hour, iter) => {
        (hour, iter.size)
      }
    }.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }

}

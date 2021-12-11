package com.study.core01WC

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark02_WordCount {
  def main(args: Array[String]): Unit = {

    //Spark框架
    //TODO 建立和spark框架的链接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    //TODO 执行业务操作
    val lines: RDD[String] = sc.textFile("datas/*")

    val words: RDD[String] = lines.flatMap(_.split(" "))

    val wordToOne: RDD[(String, Int)] = words.map(word => (word, 1))

    val wordGroup: RDD[(String, Iterable[(String, Int)])] = wordToOne.groupBy(tuple => tuple._1)

    val wordToCount: RDD[(String, Int)] = wordGroup.map {
      case (word, list) => {
        list.reduce((t1, t2) => {
          (t1._1, t1._2 + t2._2)
        })
      }
    }

    val array: Array[(String, Int)] = wordToCount.collect()
    array.foreach(println)
    //TODO 关闭链接
    sc.stop()
  }
}

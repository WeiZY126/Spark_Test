package com.study.core03RDD.dependency

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark01_RDD_Dep {
  def main(args: Array[String]): Unit = {
    //Application
    //Spark框架
    //TODO 建立和spark框架的链接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    //TODO 执行业务操作
    val lines: RDD[String] = sc.textFile("datas/word.txt")
    println(lines.toDebugString)
    println("***************")

    val words: RDD[String] = lines.flatMap(_.split(" "))
    println(words.toDebugString)
    println("***************")

    val wordToOne: RDD[(String, Int)] = words.map(word => (word, 1))
    println(wordToOne.toDebugString)
    println("***************")
    val wordToCount: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)
    println(wordToCount.toDebugString)
    println("***************")

    val array: Array[(String, Int)] = wordToCount.collect()
    array.foreach(println)
    //TODO 关闭链接
    sc.stop()
  }
}

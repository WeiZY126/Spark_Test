package com.study.core03RDD.dependency

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Dep {
  def main(args: Array[String]): Unit = {
    //Application
    //Spark框架
    //TODO 建立和spark框架的链接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    //TODO 执行业务操作
    val lines: RDD[String] = sc.textFile("datas/word.txt")
    println(lines.dependencies)
    println("***************")

    val words: RDD[String] = lines.flatMap(_.split(" "))
    println(words.dependencies)
    println("***************")

    val wordToOne: RDD[(String, Int)] = words.map(word => (word, 1))
    println(wordToOne.dependencies)
    println("***************")
    val wordToCount: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)
    println(wordToCount.dependencies)
    println("***************")

    val array: Array[(String, Int)] = wordToCount.collect()
    array.foreach(println)
    //TODO 关闭链接
    sc.stop()
  }
}

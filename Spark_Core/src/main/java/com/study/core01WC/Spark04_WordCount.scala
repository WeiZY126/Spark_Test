package com.study.core01WC

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark04_WordCount {
  def main(args: Array[String]): Unit = {
    //Application
    //Spark框架
    //TODO 建立和spark框架的链接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    //TODO 执行业务操作


    //TODO 关闭链接
    sc.stop()
  }

  //groupBy
  def wordCount1(sc: SparkContext) = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val group: RDD[(String, Iterable[String])] = words.groupBy(words => words)
    val wordCount: RDD[(String, Int)] = group.mapValues(iter => iter.size)
  }

  //groupByKey
  def wordCount2(sc: SparkContext) = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map((_, 1))
    val group: RDD[(String, Iterable[Int])] = wordOne.groupByKey()
    val wordCount: RDD[(String, Int)] = group.mapValues(iter => iter.size)
  }

  //reduceByKey
  def wordCount3(sc: SparkContext) = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map((_, 1))
    val reduceRDD: RDD[(String, Int)] = wordOne.reduceByKey(_ + _)
  }

  //aggregateByKey
  def wordCount4(sc: SparkContext) = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map((_, 1))
    wordOne.aggregateByKey(0)(_ + _, _ + _)
  }

  //foldByKey
  def wordCount5(sc: SparkContext) = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map((_, 1))
    wordOne.foldByKey(0)(_ + _)
  }

  //combineByKey
  def wordCount6(sc: SparkContext) = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map((_, 1))
    wordOne.combineByKey(v => v, (x: Int, y) => x + y, (x: Int, y: Int) => x + y)
  }

  //countByKey
  def wordCount7(sc: SparkContext) = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map((_, 1))
    wordOne.countByKey()
  }

  //countByKey
  def wordCount8(sc: SparkContext) = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    words.countByValue()
  }

  //reduce,aggregate,fold
  def wordCount9(sc: SparkContext) = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val mapWord: RDD[mutable.Map[String, Int]] = words.map(word => {mutable.Map[String, Int](word->1)})
    val reduce: mutable.Map[String, Int] = mapWord.reduce((map1, map2) => {
      map2.foreach {
        case (word, count) => {
          val newCount: Int = map1.getOrElse(word, 0) + count
          map1.update(word, count)
        }
      }
      map1
    })
  }

}

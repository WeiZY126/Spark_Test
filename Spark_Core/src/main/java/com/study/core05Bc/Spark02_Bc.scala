package com.study.core05Bc

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark02_Bc {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    val map = mutable.Map(("a", 4), ("b", 5), ("c", 6))
    //封装广播变量
    val bc: Broadcast[mutable.Map[String, Int]] = sc.broadcast(map)
    rdd1.map {
      case (word, count) => {
        //访问广播变量
        val i: Int = bc.value.getOrElse(word, 0)
        (word, (count, i))
      }
    }.collect().foreach(println)
    sc.stop()
  }
}

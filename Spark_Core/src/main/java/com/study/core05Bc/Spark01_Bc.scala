package com.study.core05Bc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark01_Bc {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 4), ("b", 5), ("c", 6)))

    //join会导致数据量的几何增长，并且会影响shuffle的性能，不推荐使用
    //    val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
    //
    //    //(a,(1,4)),(b,(2,5)),(c,(3,6))
    //    joinRDD.collect().foreach(println)
    val map = mutable.Map(("a", 4), ("b", 5), ("c", 6))
    rdd1.map {
      case (word, count) => {
        val i: Int = map.getOrElse(word, 0)
        (word, (count, i))
      }
    }.collect().foreach(println)
    sc.stop()
  }
}

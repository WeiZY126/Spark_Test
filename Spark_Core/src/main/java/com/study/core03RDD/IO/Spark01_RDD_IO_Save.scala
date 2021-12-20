package com.study.core03RDD.IO

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object Spark01_RDD_IO_Save {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    //TODO-行动算子
    rdd.saveAsTextFile("output")
    rdd.saveAsObjectFile("output2")
    rdd.saveAsSequenceFile("output3")
    sc.stop()
  }
}

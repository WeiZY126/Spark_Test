package com.study.core03RDD.partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

object Spark01_RDD_partitioner {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[(String, String)] = sc.makeRDD(List(("nba", "xxxxxx"), ("cba", "xxxxxxxxxx"), ("wnba", "xxxxxxxx"), ("nba", "xxxxxxx")), 3)
    //TODO-行动算子
    val partRDD: RDD[(String, String)] = rdd.partitionBy(new MyPartitioner)

    partRDD.saveAsTextFile("output")
    sc.stop()
  }

  /**
   * 自定义分区器
   */
  class MyPartitioner extends Partitioner {
    //分区数量
    override def numPartitions: Int = 3

    //返回数据的分区索引，从0开始
    override def getPartition(key: Any): Int = {
      key match {
        case "nba" => 0
        case "wnba" => 1
        case _ => 2
      }
    }

  }
}

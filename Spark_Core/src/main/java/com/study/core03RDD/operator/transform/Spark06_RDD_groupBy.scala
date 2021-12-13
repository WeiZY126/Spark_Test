package com.study.core03RDD.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark06_RDD_groupBy {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    //TODO 算子-groupBy
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    //groupBy会将数据源中的每一个数据进行分组判断，根据返回的分组key进行分组
    //相同的key会放在同一个组中
    def groupByFunction(num: Int): Int = {
      num % 2
    }

    val groupRDD: RDD[(Int, Iterable[Int])] = rdd.groupBy(groupByFunction(_))

    groupRDD.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }

}

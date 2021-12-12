package com.study.core03RDD.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_File_Par2 {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    //TODO 数据分区的分配
    //14个字节
    //goalSize=14/2=7
    //[0,7]   =>1234567@@   //按行读取，0->8
    //[7,14]  =>89@@0

    //如果数据源为多个文件，计算分区时以文件为单位
    val rdd: RDD[String] = sc.textFile("datas/word.txt", 2)

    rdd.saveAsTextFile("output")
    //TODO 关闭环境
    sc.stop()
  }

}

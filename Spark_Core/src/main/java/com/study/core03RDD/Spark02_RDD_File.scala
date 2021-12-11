package com.study.core03RDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    //TODO 创建RDD
    //从文件中创建RDD，将文件的数据作为处理的数据源
    //路径默认是以当前环境的根路径为基准，可以写绝对路径，也可以写相对路径
    //path可以是文件的具体路径，也可以是目录
    //    val rdd: RDD[String] = sc.textFile("datas")
    //路径还可以使用通配符
    val rdd: RDD[String] = sc.textFile("datas/1.*")
    //path还可以是分布式存储路径，如HDFS
    //    val rdd: RDD[String] = sc.textFile("hdfs://namenode/")

    rdd.collect().foreach(println)
    //TODO 关闭环境
    sc.stop()
  }

}

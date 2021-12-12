package com.study.core03RDD.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_File_Par {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    //TODO 创建RDD
    //默认也可以设定分区
    //minPartitions:最小分区数量
    //math.min(defaultParallelism, 2)
    //    val rdd: RDD[String] = sc.textFile("datas/1.*")

    //如果不想使用默认分区数量，可以使用第二个参数设置
    //spark读取文件底层使用的是hadoop的读取方式
    //分区数量计算方式
    //totalSize=7
    //goalSize=7/2=3(byte)

    //7/3=2余1 (1.1机制)，如果剩余字节数大于分区*1.1,产生一个新分区
    val rdd: RDD[String] = sc.textFile("datas/1.*", 2)

    rdd.saveAsTextFile("output")
    //TODO 关闭环境
    sc.stop()
  }

}

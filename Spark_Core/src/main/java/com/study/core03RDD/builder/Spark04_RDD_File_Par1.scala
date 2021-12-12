package com.study.core03RDD.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_File_Par1 {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    //TODO 数据分区的分配
    //1.数据以行为单位进行读取
    //spark读取文件采用的是hadoop的方式读取，所以是一行一行读取，和字节数没有关系
    //2.数据读取时以偏移量为单位,偏移量不会被重复读取
    /*
    1/t/n   =>012
    2/t/n   =>345
    3       =>6
     */
    //3.数据分区的偏移量范围计算
    //0=>[0,3]  =>1@@2@@  按行为单位4,5也会被读取
    //1=>[3,6]  =>3
    //2=>[6,7]  =>

    //【1,2】【3】【】
    val rdd: RDD[String] = sc.textFile("datas/1.*", 2)

    rdd.saveAsTextFile("output")
    //TODO 关闭环境
    sc.stop()
  }

}

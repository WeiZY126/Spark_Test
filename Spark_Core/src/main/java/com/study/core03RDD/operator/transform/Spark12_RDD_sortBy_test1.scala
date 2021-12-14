package com.study.core03RDD.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark12_RDD_sortBy_test1 {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    //TODO 算子-sortBy
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("1", 1), ("11", 2), ("2", 3)), 2)

    //sortBy方法可以根据指定的规则对数据源中的数据进行排序，默认为升序，第二个参数可以改变排序方式
    //sortBy默认情况下不会改变分区，但是中间存在shuffle操作
    val sortRDD: RDD[(String, Int)] = rdd.sortBy(num => num._1,false)
    sortRDD.collect().foreach(println)


    //TODO 关闭环境
    sc.stop()
  }

}

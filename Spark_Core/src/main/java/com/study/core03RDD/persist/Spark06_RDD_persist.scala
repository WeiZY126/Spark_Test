package com.study.core03RDD.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_persist {
  def main(args: Array[String]): Unit = {
    //cache:将数据临时存储在内存中进行数据重用
    //    会在血缘关系中添加新的依赖，一旦出现了问题，可以重头读取数据
    //persist:将数据临时存储在磁盘文件中进行重用，涉及到磁盘IO，性能较低，但是安全
    //    如果作业执行完毕，临时保存的数据文件就会丢失
    //checkpoint:将数据长久地保存在磁盘文件中进行数据重用
    //    为了保证数据安全，所以一般情况下，会独立执行作业
    //    为了能够提高效率，一般情况下是需要和cache联合使用(先cache，再checkpoint)
    //    执行过程中会切断血缘关系，重新建立新的血缘关系
    //    checkpoint等同于切断数据源
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    sc.setCheckpointDir("checkpoint")

    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    //TODO-行动算子
    val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))

    val mapRDD: RDD[(String, Int)] = flatRDD.map(word => {
      (word, 1)
    })

    //cache默认持久化的操作，只能将数据保存到内存中，如果想要使用磁盘，需使用persist
//    mapRDD.cache()
    mapRDD.checkpoint()
    println(mapRDD.toDebugString)
    //持久化操作必须在行动算子执行时完成的

    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    reduceRDD.collect().foreach(println)
    println(mapRDD.toDebugString)

    sc.stop()
  }
}

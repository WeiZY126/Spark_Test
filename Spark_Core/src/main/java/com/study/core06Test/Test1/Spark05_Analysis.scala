package com.study.core06Test.Test1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_Analysis {
  def main(args: Array[String]): Unit = {
    //TODO-Top10热门品类
    val sparkConf: SparkConf = new SparkConf().setMaster("local[3]").setAppName("Top10")
    val sc = new SparkContext(sparkConf)

    //Q:存在大量的ReduceByKey，存在大量shuffle
    //reduceByKey聚合算子，spark会提供优化，自动缓存

    //1.读取原始日志
    val actionRDD: RDD[String] = sc.textFile("datas/user_visit_action.txt")
    //放入缓存
    actionRDD.cache()

    //2.将数据转换结构
    //    点击:(ID,(1,0,0))
    //    下单:(ID,(0,1,0))
    //    支付:(ID,(0,0,1))
    val flatRDD: RDD[(String, (Int, Int, Int))] = actionRDD.flatMap(action => {
      val datas: Array[String] = action.split("_")
      if (datas(6) != "-1")
        List((datas(6), (1, 0, 0)))
      else if (datas(8) != "null") {
        val ids: Array[String] = datas(8).split(",")
        ids.map(id => (id, (0, 1, 0)))
      }
      else if (datas(10) != "null") {
        val ids: Array[String] = datas(10).split(",")
        ids.map(id => (id, (0, 0, 1)))
      }
      else
        Nil
    })
    //3.将相同品类ID的数据进行分组聚合
    //    (ID,(点击数量,下单数量,支付数量))
    val analysisRDD: RDD[(String, (Int, Int, Int))] = flatRDD.reduceByKey((t1, t2) => (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3))
    //4.将统计结果根据数量进行降序排列，取前十名

    //5.将品类进行排序，并取前十
    //点击数量排序，下单数量排序，支付数量排序
    //元组排序:先比较第一个，再比较第二个，再比较第三个
    //cogroup=connect+group
    //  （品类ID(点击数量，下单数量，支付数量)）
    val resultRDD: Array[(String, (Int, Int, Int))] = analysisRDD.sortBy(_._2, false).take(10)
    //6.将结果采集到控制台打印出来
    resultRDD.foreach(println)
    sc.stop()
  }

}

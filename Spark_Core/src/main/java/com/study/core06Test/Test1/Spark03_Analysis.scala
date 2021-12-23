package com.study.core06Test.Test1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Analysis {
  def main(args: Array[String]): Unit = {
    //TODO-Top10热门品类
    val sparkConf: SparkConf = new SparkConf().setMaster("local[3]").setAppName("Top10")
    val sc = new SparkContext(sparkConf)
    //1.读取原始日志
    val actionRDD: RDD[String] = sc.textFile("datas/user_visit_action.txt")

    //2.统计品类的点击数量:（ID,点击数量）
    val clickActionRDD: RDD[String] = actionRDD.filter(action => {
      val datas: Array[String] = action.split("_")
      !datas(6).equals("-1")
    })
    val clickCountRDD: RDD[(String, Int)] = clickActionRDD.map(action => {
      val datas: Array[String] = action.split("_")
      (datas(6), 1)
    }).reduceByKey(_ + _)

    //3.统计品类的下单数量:（ID,下单数量）
    val orderActionRDD: RDD[String] = actionRDD.filter(action => {
      val datas: Array[String] = action.split("_")
      !datas(6).equals("null")
    })
    val orderCountRDD: RDD[(String, Int)] = orderActionRDD.flatMap(action => {
      val datas: Array[String] = action.split("_")
      val cids: Array[String] = datas(8).split(",")
      cids.map((_, 1))
    }).reduceByKey(_ + _)

    //4.统计品类的支付数量:（ID,支付数量）
    val payActionRDD: RDD[String] = actionRDD.filter(action => {
      val datas: Array[String] = action.split("_")
      !datas(10).equals("null")
    })
    val payCountRDD: RDD[(String, Int)] = payActionRDD.flatMap(action => {
      val datas: Array[String] = action.split("_")
      val cids: Array[String] = datas(10).split(",")
      cids.map((_, 1))
    }).reduceByKey(_ + _)

    //5.将品类进行排序，并取前十
    //点击数量排序，下单数量排序，支付数量排序
    //元组排序:先比较第一个，再比较第二个，再比较第三个
    //cogroup=connect+group
    //  （品类ID(点击数量，下单数量，支付数量)）
    val coGroupRDD: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = clickCountRDD.cogroup(orderCountRDD, payCountRDD)
    val analysisRDD: RDD[(String, (Int, Int, Int))] = coGroupRDD.mapValues {
      case (clickIter, orderIter, payIter) => {
        var clickCnt = 0
        var orderCnt = 0
        var payCnt = 0
        val iter1: Iterator[Int] = clickIter.iterator
        if (iter1.hasNext) {
          clickCnt = iter1.next()
        }
        val iter2: Iterator[Int] = orderIter.iterator
        if (iter2.hasNext) {
          orderCnt = iter2.next()
        }
        val iter3: Iterator[Int] = payIter.iterator
        if (iter3.hasNext) {
          payCnt = iter3.next()
        }
        (clickCnt, orderCnt, payCnt)
      }
    }

    val resultRDD: Array[(String, (Int, Int, Int))] = analysisRDD.sortBy(_._2, false).take(10)
    //6.将结果采集到控制台打印出来
    resultRDD.foreach(println)
    sc.stop()
  }

}

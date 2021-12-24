package com.study.core06Test.Test3

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat
import java.util.Date
import scala.Console.println
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object spark01_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[3]").setAppName("test1")
    val sc = new SparkContext(sparkConf)

    //TODO-单跳转化率
    val datas: RDD[String] = sc.textFile("datas/user_visit_action.txt")
    //1.将数据转化为(sessionID,(时间,事件))
    val mapRDD: RDD[(String, (Date, String))] = datas.map(action => {
      val datas: Array[String] = action.split("_")
      val sessionID: String = datas(2)
      val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val date: Date = dateFormat.parse(datas(4))
      if (datas(5) != "null") {
        (sessionID, (date, "search"))
      } else if (datas(6) != "-1")
        (sessionID, (date, "click"))
      else if (datas(8) != "null")
        (sessionID, (date, "order"))
      else if (datas(10) != "-1")
        (sessionID, (date, "pay"))
      else
        ("", (new Date, ""))
    })
    //2.根据不同用户分组，根据时间排序
    val groupRDD: RDD[(String, Iterable[(Date, String)])] = mapRDD.groupByKey()
    val sortRDD: RDD[(String, List[(Date, String)])] = groupRDD.mapValues(iter => {
      iter.toList.sortBy(_._1)
    })
    //3.去除时间，保留(sessionID,事件)
    val sessionAction: RDD[(String, List[String])] = sortRDD.map(tuple => {
      (tuple._1, tuple._2.map(_._2))
    })
    //4.计算分母，计算各个事件的出现次数(搜索，点击，订单)
    val tuple1: (Int, Int, Int) = sessionAction.map(tuple => {
      var search: Int = 0
      var click: Int = 0
      var order: Int = 0
      tuple._2.foreach(action => {
        if (action == "search")
          search += 1
        else if (action == "click")
          click += 1
        else if (action == "order")
          order += 1
      })
      (search, order, click)
    }).reduce((t1, t2) => (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3))
    println(tuple1)
    //5.计算分子(搜索->点击，点击->订单，订单->支付)
    val tuple2: (Int, Int, Int) = sessionAction.map(tuple => {
      var searchToClick: Int = 0
      var clickToOrder: Int = 0
      var orderToPay: Int = 0
      val list: List[String] = tuple._2
      //            for (i <- 0 until list.size) {
      //              if (list(i) == "search" && i != list.size - 1 && list(i + 1) == "click")
      //                searchToClick += 1
      //              else if (list(i) == "click" && i != list.size - 1 && list(i + 1) == "order")
      //                clickToOrder += 1
      //      //        else if (list(i) == "order" && i != list.size - 1 && list(i + 1) == "pay")
      //              else if (list(i) == "pay")
      //                orderToPay += 1
      //            }

      list.sliding(2).foreach(list => {
        if (list.size == 1) {
          if (list(0) == "pay")
            orderToPay += 1
        }
        else if (list(0) == "search" && list(1) == "click")
          searchToClick += 1
        else if (list(0) == "click" && list(1) == "order")
          clickToOrder += 1
        else if (list(0) == "order" && list(1) == "pay")
          orderToPay += 1
      })
      (searchToClick, clickToOrder, orderToPay)
    }).reduce((t1, t2) => (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3))
    println(tuple2)
    //6.计算结果，分子分母相除
    println(s"搜索->点击:${tuple2._1.toDouble / tuple1._1.toDouble} \n\r点击->订单:${tuple2._2.toDouble / tuple1._2.toDouble} \n\r订单->支付:${tuple2._3.toDouble / tuple1._3.toDouble}")
    sc.stop()
  }

}

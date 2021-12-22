package com.study.core06Test.Test2

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.Console.println
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object spark01_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[3]").setAppName("test1")
    val sc = new SparkContext(sparkConf)

    val datas: RDD[String] = sc.textFile("datas/user_visit_action.txt")
    val zeroTupleRDD: RDD[(String, String, String, String)] = datas.map(data => {
      val strings: Array[String] = data.split("_")
      (strings(6), strings(8), strings(10), strings(2))
    })
    val hotAcc: HotAcc = new HotAcc()
    //注册累加器
    sc.register(hotAcc, "hotAcc")

    val mapRDD: RDD[(String, (Int, Int, Int, String))] = zeroTupleRDD.mapPartitions(iter => {
      val list: ListBuffer[(String, (Int, Int, Int, String))] = mutable.ListBuffer()
      iter.foreach {
        case (click, "null", "null", session) if (click != "-1") => {
          click.split(",").foreach(str =>
            list.append((str, (1, 0, 0, session)))
          )
        }
        case ("-1", order, "null", session) => {
          order.split(",").foreach(str =>
            list.append((str, (0, 1, 0, session)))
          )
        }
        case ("-1", "null", pay, session) => {
          pay.split(",").foreach(str =>
            list.append((str, (0, 0, 1, session)))
          )
        }
        case _ =>
      }
      list.iterator
    })

    mapRDD.foreach(hotAcc.add(_))
    val topList: List[(String, ((Int, Int, Int), ListBuffer[(String, Int)]))] = hotAcc.value.toList.sortBy(_._2._1)(Ordering[(Int, Int, Int)].reverse).take(10)

    val topRDD: RDD[(String, ((Int, Int, Int), ListBuffer[(String, Int)]))] = sc.makeRDD(topList)
    topRDD.mapPartitions(iter => {
      val list: ListBuffer[(String, (String, Int))] = ListBuffer()
      iter.foreach(tup => {
        val stringToInt: Map[String, Int] = tup._2._2.groupBy(_._1).map(session => {
          (session._1, session._2.length)
        })
        stringToInt.foreach(tup1 => list.append((tup._1, (tup1._1, tup1._2))))
      })
      list.iterator
    }).groupByKey().map(dates=>{
      (dates._1,dates._2.toList.sortBy(_._2)(Ordering[Int].reverse).take(10))
    }).collect().foreach(println)
    sc.stop()
  }

  /**
   * 热度累加器
   */

  import mutable.Map

  class HotAcc extends AccumulatorV2[(String, (Int, Int, Int, String)), Map[String, ((Int, Int, Int), ListBuffer[(String, Int)])]] {
    private val hotMap: mutable.Map[String, ((Int, Int, Int), ListBuffer[(String, Int)])] = Map()

    override def isZero: Boolean = hotMap.isEmpty

    override def copy(): AccumulatorV2[(String, (Int, Int, Int, String)), mutable.Map[String, ((Int, Int, Int), ListBuffer[(String, Int)])]] = new HotAcc

    override def reset(): Unit = hotMap.clear()

    override def add(value: (String, (Int, Int, Int, String))): Unit = {
      val tuple: ((Int, Int, Int), ListBuffer[(String, Int)]) = hotMap.getOrElse(value._1, ((0, 0, 0), null))
      if (tuple._2 != null) {
        tuple._2.append((value._2._4, 1))
        hotMap.update(value._1, ((value._2._1 + tuple._1._1, value._2._2 + tuple._1._2, value._2._3 + tuple._1._3), tuple._2))
      } else {
        val sessionList: ListBuffer[(String, Int)] = ListBuffer()
        sessionList.append((value._2._4, 1))
        hotMap.update(value._1, ((value._2._1 + tuple._1._1, value._2._2 + tuple._1._2, value._2._3 + tuple._1._3), sessionList))
      }
    }

    override def merge(other: AccumulatorV2[(String, (Int, Int, Int, String)), mutable.Map[String, ((Int, Int, Int), ListBuffer[(String, Int)])]]): Unit = {
      val newMap: mutable.Map[String, ((Int, Int, Int), ListBuffer[(String, Int)])] = other.value
      newMap.foreach(value => {
        val tuple: ((Int, Int, Int), ListBuffer[(String, Int)]) = hotMap.getOrElse(value._1, ((0, 0, 0), null))
        if (tuple._2 != null) {
          tuple._2 ++= value._2._2
          hotMap.update(value._1, ((value._2._1._1 + tuple._1._1, value._2._1._2 + tuple._1._2, value._2._1._3 + tuple._1._3), tuple._2))
        } else {
          val sessionList: ListBuffer[(String, Int)] = value._2._2
          hotMap.update(value._1, ((value._2._1._1 + tuple._1._1, value._2._1._2 + tuple._1._2, value._2._1._3 + tuple._1._3), sessionList))
        }
      })
    }

    override def value: mutable.Map[String, ((Int, Int, Int), ListBuffer[(String, Int)])] = hotMap
  }

}

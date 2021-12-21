package com.study.core06Test.Test1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.Console.println
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object spark01_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[3]").setAppName("test1")
    val sc = new SparkContext(sparkConf)

    val datas: RDD[String] = sc.textFile("datas/user_visit_action.txt")
    val zeroTupleRDD: RDD[(String, String, String)] = datas.map(data => {
      val strings: Array[String] = data.split("_")
      (strings(6), strings(8), strings(10))
    })
    val caseRDD: RDD[(String, (Int, Int, Int))] = zeroTupleRDD.mapPartitions(iter => {
      val list: List[(String, String, String)] = iter.toList
      val returnList: ListBuffer[(String, (Int, Int, Int))] = mutable.ListBuffer()
      list.foreach {
        case (click, "null", "null") if(click != "-1")=> {
          val clickList: Array[String] = click.split(",")
          clickList.foreach(click =>
            returnList.append((click, (1, 0, 0)))
          )
        }
        case ("-1", order, "null") => {
          val orderList: Array[String] = order.split(",")
          orderList.foreach(order =>
            returnList.append((order, (0, 1, 0)))
          )
        }
        case ("-1", "null", pay) => {
          val payList: Array[String] = pay.split(",")
          payList.foreach(pay =>
            returnList.append((pay, (0, 0, 1)))
          )
        }
      }
      returnList.iterator
    })

    caseRDD.reduceByKey((t1, t2) => (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)).sortBy(_._2, false).take(10).foreach(println)
    sc.stop()
  }

}

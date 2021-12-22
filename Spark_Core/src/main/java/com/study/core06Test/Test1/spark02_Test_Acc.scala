package com.study.core06Test.Test1

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.Console.println
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object spark02_Test_Acc {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[3]").setAppName("test1")
    val sc = new SparkContext(sparkConf)

    val datas: RDD[String] = sc.textFile("datas/user_visit_action.txt")
    val zeroTupleRDD: RDD[(String, String, String)] = datas.map(data => {
      val strings: Array[String] = data.split("_")
      (strings(6), strings(8), strings(10))
    })
    val hotAcc: HotAcc = new HotAcc()
    //注册累加器
    sc.register(hotAcc, "hotAcc")

    val mapRDD: RDD[(String, (Int, Int, Int))] = zeroTupleRDD.mapPartitions(iter => {
      val list: ListBuffer[(String, (Int, Int, Int))] = mutable.ListBuffer[(String, (Int, Int, Int))]()
      iter.foreach {
        case (click, "null", "null") if (click != "-1") => {
          click.split(",").foreach(str =>
            list.append((str, (1, 0, 0)))
          )
        }
        case ("-1", order, "null")=> {
          order.split(",").foreach(str =>
            list.append((str, (0, 1, 0)))
          )
        }
        case ("-1", "null", pay)=> {
          pay.split(",").foreach(str =>
            list.append((str, (0, 0, 1)))
          )
        }
        case _ =>
      }
      list.iterator
    })

    mapRDD.foreach(hotAcc.add(_))

    hotAcc.value.toList.sortBy(_._2)(Ordering[(Int,Int,Int)].reverse).take(10).foreach(println)

    sc.stop()
  }

  /**
   * 热度累加器
   */

  import mutable.Map

  class HotAcc extends AccumulatorV2[(String, (Int, Int, Int)), Map[String, (Int, Int, Int)]] {
    private val hotMap: mutable.Map[String, (Int, Int, Int)] = Map[String, (Int, Int, Int)]()

    override def isZero: Boolean = hotMap.isEmpty

    override def copy(): AccumulatorV2[(String, (Int, Int, Int)), mutable.Map[String, (Int, Int, Int)]] = new HotAcc

    override def reset(): Unit = hotMap.clear()

    override def add(newTuple: (String, (Int, Int, Int))): Unit = {
      val tmpTuple: (Int, Int, Int) = hotMap.getOrElse(newTuple._1, (0, 0, 0))
      hotMap.update(newTuple._1, (tmpTuple._1 + newTuple._2._1, tmpTuple._2 + newTuple._2._2, tmpTuple._3 + newTuple._2._3))
    }

    override def merge(otherAcc: AccumulatorV2[(String, (Int, Int, Int)), mutable.Map[String, (Int, Int, Int)]]): Unit = {
      val otherMap: mutable.Map[String, (Int, Int, Int)] = otherAcc.value
      otherMap.foreach(tup => {
        val tmpTuple: (Int, Int, Int) = hotMap.getOrElse(tup._1, (0, 0, 0))
        hotMap.update(tup._1, (tmpTuple._1 + tup._2._1, tmpTuple._2 + tup._2._2, tmpTuple._3 + tup._2._3))
      })

    }

    override def value: mutable.Map[String, (Int, Int, Int)] = hotMap
  }

}

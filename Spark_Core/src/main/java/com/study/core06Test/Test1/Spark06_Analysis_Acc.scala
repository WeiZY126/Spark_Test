package com.study.core06Test.Test1

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark06_Analysis_Acc {
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

    val acc: HotCategoryAccumulator = new HotCategoryAccumulator
    sc.register(acc, "hotAcc")

    //2.将数据转换结构
    actionRDD.foreach(action => {
      val datas: Array[String] = action.split("_")
      if (datas(6) != "-1")
        acc.add((datas(6), "click"))
      else if (datas(8) != "null") {
        val ids: Array[String] = datas(8).split(",")
        ids.foreach(id => acc.add((id, "order")))
      }
      else if (datas(10) != "null") {
        val ids: Array[String] = datas(10).split(",")
        ids.foreach(id => acc.add((id, "pay")))
      }
      else
        Nil
    })

    //5.将品类进行排序，并取前十
    val accVal: mutable.Map[String, HotCategory] = acc.value
    val categories: mutable.Iterable[HotCategory] = accVal.map(_._2)
    val sort: List[HotCategory] = categories.toList.sortWith((left, right) => {
      if (left.clickCnt > right.clickCnt) {
        true
      } else if (left.clickCnt == right.clickCnt) {
        if (left.orderCnt > right.orderCnt) {
          true
        } else if (left.orderCnt == right.orderCnt) {
          if (left.payCnt > right.payCnt) {
            true
          } else {
            false
          }
        } else {
          false
        }
      } else {
        false
      }
    })
    sort.take(10).foreach(println)
    sc.stop()
  }

  case class HotCategory(cid: String, var clickCnt: Int, var orderCnt: Int, var payCnt: Int)

  /**
   * 累加器
   * 1.继承AccumulatorV2
   * IN:(品类ID,行为类型)
   * OUT:mutable.Map[String,HotCategory]
   */
  class HotCategoryAccumulator extends AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] {
    private val hcMap: mutable.Map[String, HotCategory] = mutable.Map[String, HotCategory]()

    override def isZero: Boolean = hcMap.isEmpty

    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = new HotCategoryAccumulator

    override def reset(): Unit = hcMap.clear()

    override def add(v: (String, String)): Unit = {
      val cid: String = v._1
      val actionType: String = v._2
      val category: HotCategory = hcMap.getOrElse(cid, HotCategory(cid, 0, 0, 0))
      if (actionType == "click") {
        category.clickCnt += 1
      } else if (actionType == "pay") {
        category.payCnt += 1
      } else if (actionType == "order") {
        category.orderCnt += 1
      }
      hcMap.update(cid, category)
    }

    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
      val map1: mutable.Map[String, HotCategory] = this.hcMap
      val map2: mutable.Map[String, HotCategory] = other.value
      map2.foreach {
        case (cid, hc) => {
          val category: HotCategory = map1.getOrElse(cid, HotCategory(cid, 0, 0, 0))
          category.clickCnt += hc.clickCnt
          category.payCnt += hc.payCnt
          category.orderCnt += hc.orderCnt
          map1.update(cid, category)
        }
      }
    }

    override def value: mutable.Map[String, HotCategory] = hcMap
  }

}

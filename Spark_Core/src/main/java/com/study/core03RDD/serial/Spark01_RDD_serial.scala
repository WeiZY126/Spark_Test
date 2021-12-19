package com.study.core03RDD.serial

import com.study.core03RDD.operator.action.Spark07_RDD_Foreach_test.User
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark01_RDD_serial {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //TODO-行动算子
    val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello spark",
      "hive", "atguigu"))

    val search = new Search("h")

//    search.getMatch1(rdd).collect().foreach(println)
    search.getMatch2(rdd).collect().foreach(println)
    sc.stop()
  }

  //查询对象
  //类的构造参数其实是类的属性，构造参数需要进行闭包检测，等同于类进行闭包检测
  class Search(query:String){
    def isMatch(s: String): Boolean = {
      s.contains(query)
    }
    // 函数序列化案例
    def getMatch1 (rdd: RDD[String]): RDD[String] = {
      //rdd.filter(this.isMatch)
      rdd.filter(isMatch)
    }
    // 属性序列化案例
    def getMatch2(rdd: RDD[String]): RDD[String] = {
      //rdd.filter(x => x.contains(this.query))
//      rdd.filter(x => x.contains(query))
      val q = query
      rdd.filter(x => x.contains(q))
    }
  }
}

package com.study.core03RDD.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_RDD_Foreach_test {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List[Int]())
    //TODO-行动算子

    val user = new User()

    //Task not serializable
    //NotSerializableException: com.study.core03RDD.operator.action.Spark07_RDD_Foreach_test$User

    //RDD算子中传递的函数会包含闭包操作，那么就会进行检测功能
    //叫闭包检测功能
    rdd.foreach(num => {
      println("age=" + user.age + num)
    })


    sc.stop()
  }

  //样例类在编译时，会自动混入序列化特质（实现可序列化接口）
//  case class User(){
  class User{
    val age: Int = 30
  }

}

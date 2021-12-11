package com.study.core02Test

case class SubTask(){
  var datas:List[Int]= _
  var logic: Int => Int = _
  //  计算
  def compute(): List[Int] = {
    datas.map(logic)
  }
}

package com.study.test

case class SubTask(){
  var datas:List[Int]= _
  var logic: Int => Int = _
  //  计算
  def compute(): List[Int] = {
    datas.map(logic)
  }
}

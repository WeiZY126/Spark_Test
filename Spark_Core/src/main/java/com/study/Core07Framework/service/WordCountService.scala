package com.study.Core07Framework.service

import com.study.Core07Framework.common.TService
import com.study.Core07Framework.dao.WordCountDao
import org.apache.spark.rdd.RDD

/*
服务层
 */
class WordCountService extends TService {
  private val wordCountDao: WordCountDao = new WordCountDao

  //数据分析
  def dataAnalysis() = {
    val lines: RDD[String] = wordCountDao.readFile("datas/word.txt")

    val words: RDD[String] = lines.flatMap(_.split(" "))

    val value: RDD[(String, Iterable[String])] = words.groupBy(word => word)

    val wordToCount: RDD[(String, Int)] = value.map {
      case (word, list) => {
        (word, list.size)
      }
    }

    val array: Array[(String, Int)] = wordToCount.collect()
    array
  }
}

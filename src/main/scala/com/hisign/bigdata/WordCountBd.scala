package com.hisign.bigdata

import org.apache.spark.{SparkConf, SparkContext}

/** 原始数据：userid,itemid,score 用 \t 分隔
	* 功能：过滤分数大于 0.0001 的数据，然后按照用户分组，将其分数拼接到一起
	* lines --> (userid,List(itemid,score)) --> (userid,"score1,score2")
	*/
object WordCountBd {

	def main(args:Array[String]): Unit ={
		val sparkConf = new SparkConf().setAppName("WordCount")
		val sparkContext = new SparkContext(sparkConf)
		val input = sparkContext.textFile(args(0))
		val output = args(1).toString

		input.filter{x =>
			val fields = x.split("\t")
			fields(2).toDouble > 0.0001
		}.map{ x=>
			val fields = x.split("\t")
			(fields(0),(fields(1),fields(2)))
		}.groupByKey().map{ line =>
			val userid = line._1
			val value_list = line._2

			val value_arr = value_list.toArray
			val len = value_arr.length

			val buf = new StringBuilder();

			for( i <- 0 to len-1){
				buf ++= value_arr(i)._1
				buf.append(",")
			}

			buf ++= value_arr(len-1)._1

			userid + "\t" + buf
		}.saveAsTextFile(output)
	}

}

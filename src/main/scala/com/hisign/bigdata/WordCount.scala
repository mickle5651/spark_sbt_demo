package com.hisign.bigdata

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

	def main(args:Array[String]): Unit ={
		System.setProperty("spark.serializer","org.apache.spark.serializer.KryoSerializer")
		val conf = new SparkConf().setAppName("WC")
		val sc = new SparkContext(conf)
		val input = sc.textFile("REDEME.md")
		val pythonLines = input.filter(line=>line.contains("Python"))
		val words = pythonLines.flatMap(line => line.split(" "))
		val counts = words.map(word => (word,1)).reduceByKey(_+_)
		counts.saveAsTextFile("outputFile")
	}

}

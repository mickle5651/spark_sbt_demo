package com.hisign.bigdata

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
	* 功能：通过httpSocket从Server上拉取日志数据进行处理，每5分钟执行一次。
	* 模拟时通过：nc -lk 9999 启动一个server
	*/
object NetworkWordCount {

	def main(args:Array[String]): Unit ={

		println("====="+args(0))
		println("===="+args(1))
		if(args.length<2){
			System.err.println("Usage:NetworkWordCount <hostname> <ip>")
			System.exit(1)
		}

		val sparkConf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[2]")
		/**每15秒作为一个时间窗口，生成一个RDD*/
		val streamingContext = new StreamingContext(sparkConf,Seconds(15))
		/**Dstream: 一组RDDS*/
		val lines = streamingContext.socketTextStream(args(0),args(1).toInt,StorageLevel.MEMORY_AND_DISK_SER)
		val words = lines.flatMap(_.split(" "))
		val wordCounts = words.map(x => (x, 1))
			.reduceByKey(_ + _)
		wordCounts.print()
		/*提交到集群运行*/
//		wordCounts.saveAsTextFiles("hdfs://master:9000/spark_streaming_output","map3")
		/*测试环境*/
		wordCounts.saveAsTextFiles("spark_streaming_output","mp3")
		streamingContext.start()
		streamingContext.awaitTermination()

	}

}

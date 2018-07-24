package com.hisign.bigdata.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object TestSql {

	def main(args:Array[String]){

		 val studentSchema = StructType(collection.mutable.ArraySeq(
			StructField("Sno", StringType, nullable = false),
			StructField("Sname", StringType, nullable = false),
			StructField("Ssex", StringType, nullable = false),
			StructField("Sbirthday", StringType, nullable = false),
			StructField("Sclass", StringType, nullable = false)
		))

		 val sparkConf: SparkConf = new SparkConf().setAppName("sqltest")
		 val sc = new SparkContext(sparkConf)
		 val sqlContext = new SQLContext(sc)


		 val studentData: RDD[Row] = sc.textFile("hdfs://master:9000/sql_stu.data").map {
			lines =>
				val line = lines.split(",")
				Row(line(0), line(1), line(2), line(3), line(4))
		}


		 val studentTalbe: DataFrame = sqlContext.createDataFrame(studentData,studentSchema)
		studentTalbe.registerTempTable("Student")

		sqlContext.sql("SELECT Sno,Sname,Ssex,Sbirthday,Sclass from Student").show()

	}






}

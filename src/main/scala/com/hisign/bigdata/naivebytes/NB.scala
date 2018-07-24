package com.hisign.bigdata.naivebytes


import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

object NB {

	def main(args:Array[String]): Unit ={

		val conf = new SparkConf().setAppName("naiveByte")

		val sc = new SparkContext(conf)

		val data = sc.textFile(args(0))

		val parseData = data.map {
			line =>
				val parts = line.split(",")
				LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
		}

		println(parseData.getClass)

		/*训练集：测试集 = 6：4*/
		val splits = parseData.randomSplit(Array(0.6,0.4),seed = 11L)
		val training = splits(0)
		val test = splits(1)

		/*构建分类类，并调用run方法进行训练，training:分类数据，lambda:平滑因子*/
		val model = NaiveBayes.train(training,lambda = 1.0)

		val predictionAndLabel = test.map(p => (model.predict(p.features),p.label))
		val accuracy = 1.0 * predictionAndLabel.filter( x=> x._1 == x._2).count() / test.count()

		println("accuracy -->" + accuracy)
		println("Predictionof (0.0, 2.0, 0.0, 1.0): "+ model.predict(Vectors.dense(0.0,2.0,0.0,1.0)))
		println("Predictionof (2.0, 1.0, 0.0, 0.0): "+ model.predict(Vectors.dense(2.0,1.0,0.0,0.0)))

	}

}

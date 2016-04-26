package com.ml.TargetmanClassification

import org.apache.spark.mllib.classification._
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Administrator on 2016/4/20.
  *
  * features:点击股票类url的总次数，点击股票类url的总个数，refernce为股票类url的总次数，reference为股票类url的总个数
  * compare model: logistic regression model; SVM model; bayes model
  * finally used the bayes model
  */

object BuildModel {

  def main (args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Target").setMaster("local")
    val sc = new SparkContext(conf)

    // input the training data
    val input = sc.textFile("C:/Users/Administrator/Desktop/traningdata/Stockad/part-00000").filter(x => !x.contains("()"))

    val parsedData = input.map { line =>
      val parts = line.split(",")
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(" ").map(_.toDouble)))
    }.cache()

    val splits = parsedData.randomSplit(Array(0.6, 0.4), seed = 11L)
    val train = splits(0).cache()
    val test = splits(1).cache()


    /** **
      * 1 logistic regression model
      * ***/
    val modelLogistic = LogisticRegressionWithSGD.train(train, 200)
    modelLogistic.save(sc, "C:/Users/Administrator/Desktop/models/stockad/logisticmodel")

    val predictionAndLabels1 = test.map {
      case LabeledPoint(label, features) =>
        val prediction = modelLogistic.predict(features)
        (prediction, label)
    }
    val metrics1 = new MulticlassMetrics(predictionAndLabels1)
    val precision1 = metrics1.precision(1.0)
    val recall1 = metrics1.recall(1.0)

    /** **
      * 2 SVM regression model
      * ***/
    val modelSVM = SVMWithSGD.train(train, 200)
    modelSVM.save(sc, "C:/Users/Administrator/Desktop/models/stockad/SVMmodel")

    val predictionAndLabels2 = test.map {
      case LabeledPoint(label, features) =>
        val prediction = modelSVM.predict(features)
        (prediction, label)
    }

    val metrics2 = new MulticlassMetrics(predictionAndLabels2)
    val recall2 = metrics2.recall(1.0)
    val precision2 = metrics2.precision(1.0)


    /** ***
      * 3 NaiveBayes Model
      * ***/
    val modelBayes = NaiveBayes.train(train)
    modelBayes.save(sc, "C:/Users/Administrator/Desktop/models/stockad/Bayesmodel")

    val predictionAndLabels3 = test.map {
      case LabeledPoint(label, features) =>
        val prediction = modelBayes.predict(features)
        (prediction, label)
    }

    val metrics3 = new MulticlassMetrics(predictionAndLabels3)
    val recall3 = metrics3.recall(1.0)
    val precision3 = metrics3.precision(1.0)

    sc.stop()

  }

//        //the function of scale-down features
//        def toStandard (vec:RDD[String]): RDD[Double] = {
//          val rdd = vec.map(_.split(" ").map(_.toDouble)).map(line=>Vectors.dense(line))
//          val summary = Statistics.colStats(rdd)
//          val min = summary.min(0)
//          val max = summary.max(0)
//          val standardVec = vec.map(x => (x.toDouble - min)/(max - min))
//          standardVec
//        }

}

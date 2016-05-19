package com.ml.TargetmanClassification

import org.apache.spark.mllib.classification._
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by wangcao on 2016/4/20.
  *
  * 构建分类模型
  * features:点击股票类url的总次数，点击股票类url的总个数，refernce为股票类url的总次数，reference为股票类url的总个数
  * compare model: logistic regression model; SVM model; bayes model
  * finally used the bayes model
  */

object BuildModel {

  def main (args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Target").setMaster("local")
    val sc = new SparkContext(conf)

    // import and pre-process the training data
    val input = sc.textFile(args(0)).filter(x => !x.contains("()"))

    val parsedData = input.map { line =>
      val parts = line.split(",")
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(" ").map(_.toDouble)))
    }.cache()

    val splits = parsedData.randomSplit(Array(0.6, 0.4), seed = 11L)
    val train = splits(0).cache()
    val test = splits(1).cache()

    //1 logistic regression model
    val modelLogistic = LogisticRegressionWithSGD.train(train, 200)
    modelLogistic.save(sc, args(1))

    val predictionAndLabels1 = test.map {

      case LabeledPoint(label, features) =>
        val prediction = modelLogistic.predict(features)

        (prediction, label)
    }
    val metricsLogistic = new MulticlassMetrics(predictionAndLabels1)
    val precisionLogistic = metricsLogistic.precision(1.0)
    val recallLogistic = metricsLogistic.recall(1.0)

    //2 SVM model
    val modelSVM = SVMWithSGD.train(train, 200)
    modelSVM.save(sc, args(2))

    val predictionAndLabels2 = test.map {

      case LabeledPoint(label, features) =>
        val prediction = modelSVM.predict(features)

        (prediction, label)
    }

    val metricsSVM = new MulticlassMetrics(predictionAndLabels2)
    val recallSVM = metricsSVM.recall(1.0)
    val precisionSVM = metricsSVM.precision(1.0)


    //3 NaiveBayes Model
    val modelBayes = NaiveBayes.train(train)
    modelBayes.save(sc, args(3))

    val predictionAndLabels3 = test.map {

      case LabeledPoint(label, features) =>
        val prediction = modelBayes.predict(features)

        (prediction, label)
    }

    val metricsBayes = new MulticlassMetrics(predictionAndLabels3)
    val recallBayes = metricsBayes.recall(1.0)
    val precisionBayes = metricsBayes.precision(1.0)

    println("logistic:" + recallLogistic +"/t" + precisionLogistic + "\n" +
      "SVM" + recallSVM + "\t" + precisionSVM  + "\n" +
      "Bayes:" + recallBayes +"\t" + precisionBayes)

    sc.stop()

  }

}

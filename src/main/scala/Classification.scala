import java.text.SimpleDateFormat

import org.apache.spark.ml.Pipeline
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.ml.classification.{LogisticRegression, RandomForestClassifier}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, RegressionEvaluator}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.ml.regression.{GBTRegressor, RandomForestRegressionModel, RandomForestRegressor}
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

object Classification {

  case class Feature(userId:Int, numOfPay: Int, numOfView: Int, locationId: Int, perPay:Int, score:Int, commentCount:Int,
                     shopLevel:Int, hasBuyedAfter: Int) {
    override def toString: String = {
      userId + " " + numOfPay + " " + numOfView + " " + locationId + " " + perPay + " " + score + " " + commentCount +
        " " + shopLevel + " " + hasBuyedAfter
    }
  }

  case class RegressionFeature(userId:Int, shopId:Int, numOfPay: Int, numOfView: Int, locationId: Int, perPay:Int, score:Int, commentCount:Int,
                     shopLevel:Int, afterBuyCount: Int) {
    override def toString: String = {
      userId + " " + shopId + " "  + numOfPay + " " + numOfView + " " + locationId + " " + perPay + " " + score + " " + commentCount +
        " " + shopLevel + " " + afterBuyCount
    }
  }

  def generateCountData(sc: SparkContext):Unit = {
    //too large to cache
    val userPayStream = sc.textFile("hdfs://hdfsmaster:9000/graph/user_pay.txt", 100)
    val userViewStream = sc.textFile("hdfs://hdfsmaster:9000/graph/user_view.txt", 8)
    val format = new SimpleDateFormat("yyyy-MM-dd mm:HH:ss")
    val splitTime = format.parse("2016-07-01 00:00:00").getTime

    //generate feature numOfPay
    userPayStream.
      filter(line => format.parse(line.split(",")(2)).getTime < splitTime).
      map(line => {val parts = line.split(","); ((parts(0).toInt, parts(1).toInt), 1)}).
      reduceByKey(_ + _, 12).
      saveAsTextFile("hdfs://hdfsmaster:9000/graph/userPayCount")

    //generate feature numOfView
    userViewStream.
      filter(line => format.parse(line.split(",")(2)).getTime < splitTime).
      map(line => {val parts = line.split(","); ((parts(0).toInt, parts(1).toInt), 1)}).
      reduceByKey(_ + _, 12).
      saveAsTextFile("hdfs://hdfsmaster:9000/graph/userViewCount")
  }

  def generateClassLabel(sc: SparkContext):Unit = {
    val userPayStream = sc.textFile("hdfs://hdfsmaster:9000/graph/user_pay.txt", 100)
    val format = new SimpleDateFormat("yyyy-MM-dd mm:HH:ss")
    val splitTime = format.parse("2016-07-01 00:00:00").getTime

    //generate class label hasBuyedAfter
    userPayStream.
      filter(line => format.parse(line.split(",")(2)).getTime >= splitTime).
      map(line => {val parts = line.split(","); ((parts(0).toInt, parts(1).toInt), 1)}).
      reduceByKey((v1,v2) => v1).
      saveAsTextFile("hdfs://hdfsmaster:9000/graph/hasBuyedAfter")
  }

  def generateRegressionLabel(sc: SparkContext):Unit = {
    val userPayStream = sc.textFile("hdfs://hdfsmaster:9000/graph/user_pay.txt", 100)
    val format = new SimpleDateFormat("yyyy-MM-dd mm:HH:ss")
    val splitTime = format.parse("2016-07-01 00:00:00").getTime

    //generate class label hasBuyedAfter
    userPayStream.
      filter(line => format.parse(line.split(",")(2)).getTime >= splitTime).
      map(line => {val parts = line.split(","); ((parts(0).toInt, parts(1).toInt), 1)}).
      reduceByKey(_ + _).
      saveAsTextFile("hdfs://hdfsmaster:9000/graph/afterBuyCount")
  }

  def featureGenerate(sc: SparkContext, isClassification:Boolean):Unit = {
    //optimize:no need to shuffle
    val partitioner = new HashPartitioner(12)
    //question: data already shuffled
    val readCountData = (sc:SparkContext, fileName: String) => sc.textFile(fileName)
      .map(line => {
        val parts = line.substring(2, line.length - 1).split(",")
        ((parts(0).toInt, parts(1).substring(0, parts(1).length - 1).toInt), parts(2).toInt)
      }).partitionBy(partitioner);
    val userPay = readCountData(sc, "hdfs://hdfsmaster:9000/graph/userPayCount")
    val userView = readCountData(sc, "hdfs://hdfsmaster:9000/graph/userViewCount")
    val labelFileName = if (isClassification) "hdfs://hdfsmaster:9000/graph/hasBuyedAfter"
                        else "hdfs://hdfsmaster:9000/graph/afterBuyCount"

    val labelData = readCountData(sc, labelFileName)

    val shopInfo = sc.textFile("hdfs://hdfsmaster:9000/graph/shop_info.txt")
        .map(line => {
          //23,青岛,645,3,3,0,1,超市便利店,便利店,
          val parts = line.split(",")
          (parts(0).toInt, (parts(2).toInt, parts(3).toInt, if (parts(4).isEmpty) -1 else parts(4).toInt,
            if (parts(5).isEmpty) -1 else parts(5).toInt, parts(6).toInt))
        }).collect().toMap
    val shopInfoBR = sc.broadcast(shopInfo)
    val data = userPay.
      fullOuterJoin(labelData).
      mapValues(v => {
        //v1: payCount, v2: labelData
        (v._1.getOrElse(0), v._2.getOrElse(0))
      }).
      fullOuterJoin(userView).
      map(v => {
        val key = v._1
        val value = v._2
        val numOfPay = if (value._1.nonEmpty) value._1.get._1 else 0
        val hasBuyedAfter = if (value._1.nonEmpty) value._1.get._2 else 0
        val shopInfoData = shopInfoBR.value(key._2)
        RegressionFeature(key._1, key._2, numOfPay, value._2.getOrElse(0), shopInfoData._1, shopInfoData._2,
          shopInfoData._3, shopInfoData._4, shopInfoData._5, hasBuyedAfter)
    })
    val saveFileName = "hdfs://hdfsmaster:9000/graph/dataset" + (if (isClassification) "" else "Regression")
    data.saveAsTextFile(saveFileName)
  }

  def doClassfication(spark:SparkSession):Unit = {
    if (false) {
      generateCountData(spark.sparkContext)
      generateClassLabel(spark.sparkContext)
      featureGenerate(spark.sparkContext, true)
    }

    import spark.implicits._
    val userDF = spark.sparkContext.textFile("hdfs://hdfsmaster:9000/graph/dataset").map(_.split(" ")).map(_.map(_.toInt))
      .map(v => Feature(v(0),v(1),v(2),v(3),v(4),v(5),v(6),v(7), v(8))).toDF().persist(StorageLevel.MEMORY_AND_DISK)
    userDF.createOrReplaceTempView("user")
    userDF.printSchema()

    val featureCols = Array("userId", "numOfPay", "numOfView", "locationId", "perPay", "score", "commentCount", "shopLevel")
    val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
    val df2 = assembler.transform(userDF)

    val labelIndexer = new StringIndexer().setInputCol("hasBuyedAfter").setOutputCol("label")
    val df3 = labelIndexer.fit(df2).transform(df2)

    val splitSeed = 1234
    val Array(trainingData, testData) = df3.randomSplit(Array(0.8, 0.2), splitSeed)

    var timestart = System.currentTimeMillis()
    //我们有8个特征，所以设置4最大树高，32棵树，特征子集选择为auto，对于分类是sqrt(n)，对于回归是1/3
    val classifier = new RandomForestClassifier().setImpurity("gini")
      .setMaxDepth(4).setNumTrees(32).setFeatureSubsetStrategy("auto").setSeed(5043)
    val model = classifier.fit(trainingData)
    println("train used " + (System.currentTimeMillis() - timestart) + "ms")
    timestart = System.currentTimeMillis()
    //prediction
    val predictions = model.transform(testData)
    println("predict test used " + (System.currentTimeMillis() - timestart) + "ms")
    predictions.select("userId", "label", "prediction", "probability").show(10)
    timestart = System.currentTimeMillis()
    //default auc
    val evaluator = new BinaryClassificationEvaluator().setLabelCol("label")

    val auc = evaluator.evaluate(predictions)
    println(evaluator.metricName.name + ":" + auc)
    println("evaluate test used " + (System.currentTimeMillis() - timestart) + "ms")
    //todo ML pipeline
  }

  def doRegression(spark:SparkSession): Unit = {
    import spark.implicits._

    if (true) {
//      generateCountData(spark.sparkContext)
//      generateRegressionLabel(spark.sparkContext)
//      featureGenerate(spark.sparkContext, false)
    }

    import spark.implicits._
    val userDF = spark.sparkContext.textFile("hdfs://hdfsmaster:9000/graph/datasetRegression", 48).map(_.split(" ")).map(_.map(_.toInt))
      .map(v => RegressionFeature(v(0),v(1),v(2),v(3),v(4),v(5),v(6),v(7), v(8), v(9))).toDF()
    userDF.createOrReplaceTempView("user")
//    userDF.printSchema()

    val featureCols = Array("userId", "numOfPay", "numOfView", "locationId", "perPay", "score", "commentCount", "shopLevel")
    val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
    val df2 = assembler.transform(userDF)

    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(8) //numOfPay怕会被归到categorical里面，所以先算了
    val df3 = featureIndexer.fit(df2).transform(df2)

    val splitSeed = 1234
    //todo: testData
    val Array(trainingData, testData) = df3.sample(false, 0.2, splitSeed).randomSplit(Array(0.8, 0.2), splitSeed)
    trainingData.persist(StorageLevel.MEMORY_AND_DISK)
//    testData.persist(StorageLevel.MEMORY_AND_DISK)
    var timestart = System.currentTimeMillis()

    //训练随机森林
    val rf = new RandomForestRegressor()
      .setLabelCol("afterBuyCount")
      .setFeatureSubsetStrategy("auto")
      .setFeaturesCol("indexedFeatures").setSeed(5043)

    val rfModel = rf.fit(trainingData)
    rfModel.setPredictionCol("rfPrediction")
    val rfPredictions = rfModel.transform(testData)

    rfPredictions.persist(StorageLevel.MEMORY_AND_DISK)
    rfPredictions.printSchema()

    //训练GBTs
    val gbt = new GBTRegressor()
      .setLabelCol("afterBuyCount")
      .setFeaturesCol("indexedFeatures")
      .setMaxIter(10)
    val gbtModel = gbt.fit(trainingData)
    gbtModel.setPredictionCol("gbtPrediction")

    val gbtPredictions = gbtModel.transform(rfPredictions)

    gbtPredictions.persist(StorageLevel.MEMORY_AND_DISK)
    gbtPredictions.printSchema()
    println("gbt predict test used " + (System.currentTimeMillis() - timestart) + "ms")
//    gbtPredictions.select("prediction", "afterBuyCount", "indexedFeatures").show(10)
    // Select (prediction, true label) and compute test error.
    val gbtEvaluator = new RegressionEvaluator()
      .setLabelCol("afterBuyCount")
      .setPredictionCol("gbtPrediction")
      .setMetricName("rmse")
    val rfEvaluator = new RegressionEvaluator()
      .setLabelCol("afterBuyCount")
      .setPredictionCol("rfPrediction")
      .setMetricName("rmse")
    val gbtRmse = gbtEvaluator.evaluate(gbtPredictions)
    val rfRmse = rfEvaluator.evaluate(gbtPredictions)
    println("rf Root Mean Squared Error (RMSE) on test data = " + rfRmse)
    println("gbt Root Mean Squared Error (RMSE) on test data = " + gbtRmse)

    //用RF和GBTs的结果作为新特征、使用原标签用lr进行训练
    timestart = System.currentTimeMillis()
    //使用rdd方式
//    val mapFunc = (v:Row) => ((v.getAs[Int]("userId"), v.getAs[Int]("shopId")),
//      (v.getAs[Double]("afterBuyCount") - v.getAs[Double]("prediction"), v.getAs[Double]("afterBuyCount")))
//    val partitionFunc = new HashPartitioner(12)
//    val rfDistances = rfPredictions.map(mapFunc).rdd.partitionBy(partitionFunc)
//    val gbtDistances = gbtPredictions.map(mapFunc).rdd.partitionBy(partitionFunc)
    //scala 里面Int不能直接转成Double...
//    val lrDF = gbtPredictions.map(v => {
//      (v.getAs[Double]("rfPrediction"),
//        v.getAs[Double]("gbtPrediction"),
//        v.getAs[Int]("afterBuyCount"))
//    }).toDF("rfDistance", "gbtDistance", "afterBuyCount")

    //使用sql方式
//    rfPredictions.createGlobalTempView("rf")
//    gbtPredictions.createGlobalTempView("gbt")
//    val lrDF = spark.sqlContext.sql("SELECT userId ,shopId , " +
//      "rf.prediction - rf.afterBuyCount AS rfDistance, " +
//      "gbt.prediction - gbt.afterBuyCount AS gbtDistance " +
//      "rf.afterBuyCount afterBuyCount " +
//      "FROM rf JOIN gbt ON rf.userId = gbt.userId AND rf.shopId = gbt.shopId")
    val lrFeatureAssembler = new VectorAssembler().setInputCols(Array("rfPrediction", "gbtPrediction")).setOutputCol("lrFeatures")
    val lrFeatures = lrFeatureAssembler.transform(gbtPredictions)
    val Array(lfTrainData, lfTestData) = lrFeatures.randomSplit(Array(0.8, 0.2))

//  用LR进行训练
    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
      .setFeaturesCol("lrFeatures")
      .setLabelCol("afterBuyCount")
    val lrModel = lr.fit(lfTrainData)
    lrModel.setPredictionCol("lrPrediction")
    val lrPredictions = lrModel.transform(lfTestData)

    //评价LR模型
    val valuesAndPreds = lrPredictions.rdd.map{ point =>
      (point.getAs[Double]("lrPrediction"), point.getAs[Int]("afterBuyCount").toDouble)
    }
    // Instantiate metrics object
    val metrics = new RegressionMetrics(valuesAndPreds)
    // Squared error
//    println(s"LR MSE = ${metrics.meanSquaredError}")
//    lrPredictions.select("prediction", "afterBuyCount").show(10)
    println(s"LR RMSE = ${metrics.rootMeanSquaredError}")

//    gbtPredictions.persist(StorageLevel.MEMORY_AND_DISK)

    //gbt2
    val newFeatureCols:Array[String] = Array("userId", "numOfPay", "numOfView", "locationId", "perPay", "score",
      "commentCount", "shopLevel", "rfPrediction", "gbtPrediction")
    val gbt2Data = new VectorAssembler().setInputCols(newFeatureCols).setOutputCol("gbt2Features").transform(gbtPredictions)
    val Array(gbt2TrainData, gbt2TestData) = gbt2Data.randomSplit(Array(0.8, 0.2))
    val gbt2 = new GBTRegressor()
      .setLabelCol("afterBuyCount")
      .setFeaturesCol("gbt2Features")
      .setMaxIter(10)
    val gbt2Model = gbt2.fit(gbt2TrainData)
    gbt2Model.setPredictionCol("gbt2Prediction")
    val gbt2Predictions = gbt2Model.transform(gbt2TestData)
    gbtEvaluator.setPredictionCol("gbt2Prediction")
    println("gbt2 Root Mean Squared Error (RMSE) on test data = " + gbtEvaluator.evaluate(gbt2Predictions))


    //caculate statitics
    gbtPredictions.createGlobalTempView("lr")
    //lr not found error?
    spark.sqlContext.sql("SELECT min(afterBuyCount), max(afterBuyCount) FROM lr")
    val sum = lrPredictions.rdd.map(v => (v.getAs[Int]("afterBuyCount").toDouble, 1)).reduce((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2))
    val avg = (sum._1 / sum._2)
    println("avg:" + avg)
    val variance = lrPredictions.rdd.map(v => { val value =v.getAs[Int]("afterBuyCount") - avg;value * value})
        .reduce(_ + _) / sum._2
    println("variance:" + variance + " std:" + Math.sqrt(Math.abs(variance)))
//    lrPredictions.rdd.map(v => {
//      val trueValue = v.getAs[Double]("afterBuyCount")
//      val rfOriginValue = trueValue + v.getAs[Double]("rfDistance")
//      val lrValue = v.getAs[Double]("prediction")
//    })

    //
  }

  def main(args:Array[String]):Unit = {
    val spark = SparkSession
      .builder()
      .appName("Classification")
      .config("spark.master", "spark://sparkmaster:7077")
      .getOrCreate()

    doClassfication(spark)
//    doRegression(spark)
  }
}

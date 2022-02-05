import breeze.linalg.{*}
import breeze.stats.mean
import breeze.stats.stddev
import linearRegration.linearRegration
import dataset.data.{readData, trainTestSplit, writePredict}

import com.typesafe.scalalogging.Logger


object main extends App {
  var logger = Logger("Main")

  logger.info("Parse args")
  val (inputPath, outputPath) = (args(0), args(1)) // Parse Arguments

  logger.info("Read data")
  val (dataX, dataY) = readData(inputPath, target_index = 10)
  logger.info("Train test split")
  val (trainX, trainY, testX, testY) = trainTestSplit(dataX, dataY, trainSize = 0.8)
  logger.info(s"Train size: ${trainX.rows} , test size: ${testX.rows}")

  val model = new linearRegration(.01)
  model.fit(trainX, trainY)

  val trainPred = model.predict(trainX)
  logger.info(s"MSE on train dataset: ${model.MSE(trainY, trainPred)}")
  val testPred = model.predict(testX)
  logger.info(s"MSE on test dataset: ${model.MSE(testY, testPred)}")
  writePredict(testPred, testY, outputPath)
  logger.info(s"Result save --> ${outputPath}")
}

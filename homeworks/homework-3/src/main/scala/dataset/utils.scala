package dataset

import breeze.linalg.{DenseMatrix, DenseVector, convert, csvread, csvwrite}
import breeze.numerics.round

import java.io.File

object data {
  def readData(dataFilepath: String, target_index: Int): (DenseMatrix[Double], DenseVector[Double]) = {

    val df = csvread(new File(dataFilepath), separator = ',', skipLines = 1)

    val X = df(::, 1 until target_index)
    val Y = df(::, target_index)

    (X, Y)
  }

  def trainTestSplit(dataX: DenseMatrix[Double], dataY: DenseVector[Double], trainSize: Double):
    (DenseMatrix[Double], DenseVector[Double], DenseMatrix[Double], DenseVector[Double]) = {

    val rowLen = dataY.length
    val stopIndex = (rowLen * trainSize).toInt

    val (trainX, testX) = (dataX(0 until stopIndex, ::), dataX(stopIndex until rowLen, ::))
    val (trainY, testY) = (dataY(0 until stopIndex), dataY(stopIndex until rowLen))

    (trainX, trainY, testX, testY)
  }

  def writePredict(predictY: DenseVector[Double], testY: DenseVector[Double], outputPath: String): Unit = {
    val result = convert(DenseMatrix.horzcat(round(predictY.toDenseMatrix.t), round(testY.toDenseMatrix.t)), Double)
    csvwrite(new File(outputPath), separator = ',', mat = result)
  }
}

package linearRegration

import breeze.numerics.{pow, sqrt}
import breeze.linalg.{*, DenseMatrix, DenseVector, sum}
import breeze.stats.{mean, stddev}

import com.typesafe.scalalogging.Logger


class linearRegration (lr: Double){
  val learnRate: Double = lr
  var dataMean: DenseVector[Double] = DenseVector(0, 0, 0)
  var dataVar: DenseVector[Double] = DenseVector(0, 0, 0)
  var weights: DenseVector[Double] = DenseVector.zeros[Double](1)
  var bias: Double = 0.0
  var cost: Double = 0.0
  var step: Int = 400
  var logger: Logger = Logger("Train")

  def normalize(dataX: DenseMatrix[Double]): DenseMatrix[Double] = {
    dataMean = mean(dataX(::, *)).inner
    dataVar = stddev(dataX(::, *)).inner
    val normalizedData = dataX(*, ::) - dataMean
    (normalizedData(*, ::) / dataVar)
  }

  def computeCost(h: DenseVector[Double], Y:  DenseVector[Double]): Unit = {
    cost = sum(pow((h - Y), 2)) / (2 * h.length)
  }

  def optimize(X: DenseMatrix[Double], Y: DenseVector[Double]): Unit = {
    var h = (X * weights) + bias
    weights :-= learnRate * 2 * (X.t * (h - Y))
    weights = weights.map(el => el / X.rows)
    bias -= learnRate * 2 * sum(h - Y) / X.rows
  }

  def fit(X: DenseMatrix[Double], y: DenseVector[Double]): Unit = {
    logger.info("Data Normalize")
    val dataX = normalize(X)
    weights = DenseVector.zeros[Double](dataX.cols)
    logger.info("Learnging....")
    for (_ <- 0 to step) {
      var h = (X * weights) + bias
      computeCost(h, y)
      optimize(dataX, y)
      var pred = predict(dataX)
      logger.info(String.format("MSE: %.5f", MSE(y, pred)))
    }
  }

  def predict(X: DenseMatrix[Double]): DenseVector[Double] = {
    val dataX = normalize(X)
    (dataX * weights + bias)
  }

  def MSE(yTrue: DenseVector[Double], yPred: DenseVector[Double]): Double = {
    val error = sum((yTrue - yPred)) / yTrue.length
    sqrt(error)
  }
}

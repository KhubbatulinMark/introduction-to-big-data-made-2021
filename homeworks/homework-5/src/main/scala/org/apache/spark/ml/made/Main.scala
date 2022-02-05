package org.apache.spark.ml.made

import breeze.linalg._
import breeze.stats.distributions.RandBasis
import com.typesafe.scalalogging.Logger
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object Main extends App {
  var logger = Logger("main")
  logger.info(f"Start SparkSession")
  val spark = SparkSession.builder
    .master(s"local[1]")
    .getOrCreate()
  logger.info(f"Created SparkSession")
  import spark.sqlContext.implicits._

  val X = DenseMatrix.rand[Double](100000, 3) // Set X shape
  logger.info(s"Train shape: ${X.rows}X${X.cols}")

  val model_param = DenseVector(1.5, 0.3, -0.7).asDenseMatrix.t // Set Hidden model param
  logger.info(s"Model params: ${model_param.data}")
  val Y = (X * model_param).toDenseVector

  val train_data = Range(0, X.rows)
    .map(index => Tuple2(Vectors.fromBreeze(X(index, ::).t), Y(index)))
    .toSeq
    .toDF("features", "label")

  logger.info(s"Creating Model")
  var lr = LinearRegression
    .setNumIter(5000)
    .setstepSize(1)
  var model = lr.fit(train_data)
  logger.info(
    f"Weights: ${model.coefficients(model.coefficients.size - 1)}"
  )

  spark.stop()
}

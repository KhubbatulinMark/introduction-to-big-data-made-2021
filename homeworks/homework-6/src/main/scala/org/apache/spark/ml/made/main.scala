package org.apache.spark.ml.made

import org.apache.spark.sql.functions.{lit, monotonicallyIncreasingId}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Row, SparkSession}
import org.apache.spark.ml.{Pipeline, evaluation}
import org.apache.spark.ml.feature.{HashingTF, IDF, RegexTokenizer}


object main_function extends App {
  val spark = SparkSession.builder().master("local").getOrCreate()
  val df = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("data/tripadvisor_hotel_reviews.csv")
    .sample(0.1)

  val preprocessingPipe = new Pipeline()
    .setStages(Array(
      new RegexTokenizer()
        .setInputCol("Review")
        .setOutputCol("tokenized")
        .setPattern("\\W+"),
      new HashingTF()
        .setInputCol("tokenized")
        .setOutputCol("tf")
        .setNumFeatures(1000),
      new IDF()
        .setInputCol("tf")
        .setOutputCol("tfidf")
    ))

  val Array(train, test) = df.randomSplit(Array(0.8, 0.2))
  val pipe = preprocessingPipe.fit(train)
  val train_features = pipe.transform(train)
  val test_features = pipe.transform(test)
  val testFeaturesWithIndex = test_features.withColumn("id", monotonicallyIncreasingId())
  train_features.show(3)

  var HyperplanesLSH =  new RandomHyperplanesLSH()
    .setInputCol("tfidf")
    .setOutputCol("buckets")
    .setNumHashTables(3)

  val model = HyperplanesLSH.fit(train_features)
  val neighbors = model.approxSimilarityJoin(train_features, test_features, 0.8)
  neighbors.show()

}

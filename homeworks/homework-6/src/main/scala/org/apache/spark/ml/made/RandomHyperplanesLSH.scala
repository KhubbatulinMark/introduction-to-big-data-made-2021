package org.apache.spark.ml.made

import scala.math.{signum, sqrt}

import scala.util.Random
import org.apache.hadoop.fs.Path
import org.apache.spark.annotation.Since
import org.apache.spark.ml.feature.{LSH, LSHModel}
import org.apache.spark.ml.linalg.{Vector, VectorUDT, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.HasSeed
import org.apache.spark.ml.util._
import org.apache.spark.sql.types.StructType


class RandomHyperplanesLSHModel private[ml](
                                   override val uid: String,
                                   private[ml] val randHyperplanes: Array[Vector])
  extends LSHModel[RandomHyperplanesLSHModel] {

  override def setInputCol(value: String): this.type = super.set(inputCol, value)

  override def setOutputCol(value: String): this.type = super.set(outputCol, value)

  override protected[ml] def hashFunction(elems: Vector): Array[Vector] = {
    require(elems.nonZeroIterator.nonEmpty, "Must have at least 1 non zero entry.")
    val hashValues = randHyperplanes.map { case plane =>
      signum(
        elems.nonZeroIterator.map { case (i, v) =>
          v * plane(i)
        }.sum
      )
    }
    hashValues.map(Vectors.dense(_))
  }

  override protected[ml] def hashDistance(x: Seq[Vector], y: Seq[Vector]): Double = {
    x.iterator.zip(y.iterator).map(vectorPair =>
      vectorPair._1.toArray.zip(vectorPair._2.toArray).count(pair => pair._1 != pair._2)
    ).min
  }

  override protected[ml] def keyDistance(x: Vector, y: Vector): Double = {
    if (Vectors.norm(x, 2) == 0 || Vectors.norm(y, 2) == 0) 1.0
    else 1 - (x.dot(y) / Vectors.norm(x, 2) * Vectors.norm(y, 2))
  }

  override def copy(extra: ParamMap): RandomHyperplanesLSHModel = {
    val copied = new RandomHyperplanesLSHModel(uid, randHyperplanes).setParent(parent)
    copyValues(copied, extra)
  }

  override def write: MLWriter = new RandomHyperplanesLSHModel.RandomHyperplanesLSHModelWriter(this)

  override def toString: String = {
    s"RandomHyperplanesLSHModel: uid=$uid, numHashTables=${$(numHashTables)}"
  }

}

class RandomHyperplanesLSH(override val uid: String) extends LSH[RandomHyperplanesLSHModel] with HasSeed {

  override def setInputCol(value: String): this.type = super.setInputCol(value)
  override def setOutputCol(value: String): this.type = super.setOutputCol(value)
  override def setNumHashTables(value: Int): this.type = super.setNumHashTables(value)
  def this() = {
    this(Identifiable.randomUID("rhp-lsh"))
  }
  def setSeed(value: Long): this.type = set(seed, value)
  override protected[ml] def createRawLSHModel(dim: Int): RandomHyperplanesLSHModel = {
    val seed_planes = new Random($(seed))
    val random_planes: Array[Vector] = Array.fill($(numHashTables)) {
      Vectors.dense(Array.fill(dim)(2*seed_planes.nextDouble - 1))
    }
    new RandomHyperplanesLSHModel(uid, random_planes)
  }

  override def transformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(inputCol), new VectorUDT)
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)
}

object RandomHyperplanesLSHModel extends MLReadable[RandomHyperplanesLSHModel] {

  override def read: MLReader[RandomHyperplanesLSHModel] = new RandomHyperplanesLSHModelReader
  override def load(path: String): RandomHyperplanesLSHModel = super.load(path)

  private[RandomHyperplanesLSHModel] class RandomHyperplanesLSHModelWriter(instance: RandomHyperplanesLSHModel)
    extends MLWriter {

    private case class Data(random_planes: Array[Vector])

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.randHyperplanes)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class RandomHyperplanesLSHModelReader extends MLReader[RandomHyperplanesLSHModel] {

    private val className = classOf[RandomHyperplanesLSHModel].getName

    override def load(path: String): RandomHyperplanesLSHModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)

      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath).select("random_planes").head()
      val random_planes = data.getSeq[Vector](0).toArray
      val model = new RandomHyperplanesLSHModel(metadata.uid, random_planes)

      metadata.getAndSetParams(model)
      model
    }
  }
}
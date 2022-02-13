package com.sb.search.model.input.common.utils

import com.sb.search.model.Catalogue
import com.sb.search.model.data.IndexedWordsDataset
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext, SQLImplicits, SparkSession}

import javax.inject.Inject

class DatasetHelperService @Inject()(sparkSession: SparkSession) {

  Logger.getLogger(getClass.getName).setLevel(Level.INFO)
  private val log = LogManager.getLogger(getClass.getName)


  def getCatalogue: DataFrame = {
    log info s"reading catalogue"
    readFromS3(classOf[Catalogue])
  }

  def getIndexedWords: DataFrame = {
    log info s"reading subscription price"
    readFromS3(classOf[IndexedWordsDataset])
  }

  def getTextStreamSearch: DataFrame = {
    val path = "path to read stream built with class name";
    log info s"sending user search to rank engine"
    sparkSession.readStream.load(path)
  }

  def readFromS3[T](clazz: Class[T]): DataFrame = {
    val path = "path_from_s3 built with class name";
    log info "reading: %s  with path %s".format(clazz.getSimpleName, path)
    sparkSession.read.parquet(path)
  }

  def writeToS3[T](ds: Dataset[_], clazz: Class[T]): Unit = {
    val path = "path_to_s3 built with class name";
    log info "writing: %s  with path %s".format(clazz.getSimpleName, path)
    log info "writing to path: %s".format(path)
    ds.write.parquet(path)
  }

  def writeStreamToDelta[T](ds: Dataset[_], clazz: Class[T]): Unit = {
    val path = "path_to delta table built with class name";
    log info ("find delta table by class name: " + clazz.getSimpleName + "  with path " + path)
    log info "writing to path: %s".format(path)
    ds
      .writeStream
      .outputMode("append")
      .option("checkpointLocation", "/delta/events/_checkpoints/etl-from-json")
      .format("delta")
      .start("/delta/events")
  }

  def readModelFromS3[T](clazz: Class[T]): PipelineModel = {
    val path = "model path_from_s3 built with class name";
    log info s"reading model: ${clazz.getSimpleName}  with path $path"
    PipelineModel.load(path)
  }

  def writeModelToS3[T](pipelineModel: PipelineModel, clazz: Class[T]): Unit = {
    val path = "path_to_s3 fro model built with class name";
    log info s"writing model: ${clazz.getSimpleName}  with path $path"
    pipelineModel.save(path)
  }


}

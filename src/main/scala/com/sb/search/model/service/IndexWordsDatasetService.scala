package com.sb.search.model.service

import com.sb.search.model.config.configuration
import com.sb.search.model.data.{IndexWordsTable, IndexedWordsDataset}
import com.sb.search.model.input.common.utils.{DatasetHelperService, FunctionalInterfaceRunService}
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.ml.feature.{CountVectorizer, StopWordsRemover, Tokenizer}
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.sql.{Dataset, Row}

import javax.inject.Inject

/**
 *
 * @param datasetHelperService reads form the catalogue and returns words to be indexed
 */
class IndexWordsDatasetService @Inject()(datasetHelperService: DatasetHelperService) extends FunctionalInterfaceRunService {

  Logger.getLogger(getClass.getName).setLevel(Level.INFO)
  private val log = LogManager.getLogger(getClass.getName)

  override def run(): Unit = {

    log info s"Running service: ${this.getClass.getSimpleName}"
    val catalogue = datasetHelperService.getCatalogue
    val indexedWordsDataset = getIndexWordsDataset(catalogue)
    datasetHelperService.writeModelToS3(indexedWordsDataset, classOf[IndexedWordsDataset])
    datasetHelperService.writeToS3(indexedWordsDataset.transform(catalogue), classOf[IndexedWordsDataset])

  }

  /**
   *
   * @param text is the catalogue that contain the titles we would like to index in ES
   * @return words to be indexed per title
   */
  def getIndexWordsDataset(text: Dataset[Row]): PipelineModel = {
    val pipeline: Pipeline = new Pipeline
    pipeline.setStages(Array[PipelineStage](
      new Tokenizer()
        .setInputCol(IndexWordsTable.TEXT)
        .setOutputCol(IndexWordsTable.TOKENS),
      new StopWordsRemover()
        .setInputCol(IndexWordsTable.TOKENS)
        .setOutputCol(IndexWordsTable.FILTERED),
      new CountVectorizer()
        .setVocabSize(configuration.indexWordsMetrics.vocabularySize)
        .setInputCol(IndexWordsTable.FILTERED)
        .setOutputCol(IndexWordsTable.FEATURES)

    ))
    pipeline.fit(text)
  }

}

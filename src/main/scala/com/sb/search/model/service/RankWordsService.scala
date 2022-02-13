package com.sb.search.model.service

import com.sb.search.model.data.{IndexedWordsDataset, RankWordsEvents, RankWordsTable}
import com.sb.search.model.input.common.utils.{DatasetHelperService, FunctionalInterfaceRunService}
import com.sb.search.model.transformers.rank.{CalculateSimilaritiesTransformer, RankTransformer}
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import javax.inject.Inject

/**
 *
 * @param datasetHelperService read model from the indexed dataset and text from stream dataset
 *                             and create rank of words to be returned in search
 */
class RankWordsService  @Inject()(datasetHelperService: DatasetHelperService) extends FunctionalInterfaceRunService {

  Logger.getLogger(getClass.getName).setLevel(Level.INFO)
  private val log = LogManager.getLogger(getClass.getName)

  override def run(): Unit = {

    log info s"Running service: ${this.getClass.getSimpleName}"
    val textStreamToSearch = datasetHelperService.getTextStreamSearch
    val readModelFromS3 = datasetHelperService.readModelFromS3(classOf[IndexedWordsDataset])
    val readIndexedWordsFromS3 = datasetHelperService.readFromS3(classOf[IndexedWordsDataset])
    val inputStreamTransformed = getInputStreamTransformed(textStreamToSearch, readModelFromS3)
    val indexedWordsDataset = getRankDataset(inputStreamTransformed, readIndexedWordsFromS3)
    datasetHelperService.writeStreamToDelta(indexedWordsDataset, classOf[RankWordsEvents])

  }

  /**
   *
   * @param textTransformed is the text we want to compare, this approach could be used to find titles
   *             because it uses a similarity approach
   * @return
   */
  def getRankDataset(textTransformed: Dataset[Row], catalogueIndexed: Dataset[Row]): Dataset[Row] = {
    val pipeline: Pipeline = new Pipeline
    pipeline.setStages(Array[PipelineStage](
      new CalculateSimilaritiesTransformer(textTransformed),
      new RankTransformer
    ))
    pipeline.fit(catalogueIndexed).transform(catalogueIndexed)
  }

  private def getInputStreamTransformed(textStreamToSearch: DataFrame, readModelFromS3: PipelineModel):DataFrame = {
    readModelFromS3.transform(textStreamToSearch)
      .withColumnRenamed(RankWordsTable.FEATURES, RankWordsTable.FEATURES_SEARCH)
  }

}

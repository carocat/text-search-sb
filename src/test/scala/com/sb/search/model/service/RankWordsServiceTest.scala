package com.sb.search.model.service

import com.sb.search.model.SparkBaseTest
import com.sb.search.model.data.{CatalogueTable, IndexedWordsDataset, RankWordsTable}
import com.sb.search.model.input.common.utils.DatasetHelperService
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.mockito.Mockito.{mock, when}

class RankWordsServiceTest extends SparkBaseTest {

  val datasetHelperServiceMock: DatasetHelperService = mock(classOf[DatasetHelperService])
  val rankWordsServiceMock = new RankWordsService(datasetHelperServiceMock)


  test("testRunIndexWordsDatasetService") {
    when(datasetHelperServiceMock.readFromS3(classOf[IndexedWordsDataset])).thenReturn(getIndexedWords)
    when(datasetHelperServiceMock.getTextStreamSearch).thenReturn(getTextStream)
    when(datasetHelperServiceMock.readModelFromS3(classOf[IndexedWordsDataset])).thenReturn(getModel(datasetHelperServiceMock))
    rankWordsServiceMock.run()
  }

  test("testSimilarityAndRank") {
    val result = rankWordsServiceMock.getRankDataset(getTextSearchTransformed, getIndexedWords)
    assert(0.9934800715262958 == result.select(RankWordsTable.SIMILARITY).collectAsList().get(0).getDouble(0))

  }

  private def getModel(datasetHelperServiceMock: DatasetHelperService) = {
    new IndexWordsDatasetService(datasetHelperServiceMock).getIndexWordsDataset(getCatalogue)
  }

  //scalastyle:off
  private def getIndexedWords: Dataset[Row] = {
    sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(
        Seq(
          Row("cases are perfectly simple")
        )
      ),
      StructType(Array[StructField](
        StructField(CatalogueTable.TEXT, DataTypes.StringType, nullable = true)
      ))
    ).crossJoin(
    sparkSession.sqlContext.createDataFrame(Array(
      Vectors.sparse(10, Seq((1, 4.0), (2, 7.0)))
    ).map(Tuple1.apply)).toDF(RankWordsTable.FEATURES))
  }


  //scalastyle:off
  private def getCatalogue: Dataset[Row] = {
    sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(
        Seq(
          Row(1, "These cases are perfectly simple and easy to distinguish"),
          Row(2, "simple are perfectly cases"),
          Row(2, "cases are perfectly simple")
        )
      ),
      StructType(Array[StructField](
        StructField(CatalogueTable.ID, DataTypes.IntegerType, nullable = true),
        StructField(CatalogueTable.TEXT, DataTypes.StringType, nullable = true)
      ))
    )
  }


  //scalastyle:off
  private def getTextStream: Dataset[Row] = {
    sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(
        Seq(
          Row("cases are perfectly simple")
        )
      ),
      StructType(Array[StructField](
        StructField(CatalogueTable.TEXT, DataTypes.StringType, nullable = true)
      ))
    )
  }

  //scalastyle:off
  private def getTextSearchTransformed: DataFrame = {
    val data = Array(
      Vectors.sparse(10, Seq((1, 3.0), (2, 7.0)))
    )
    sparkSession.sqlContext.createDataFrame(data.map(Tuple1.apply)).toDF(RankWordsTable.FEATURES_SEARCH)
  }

}

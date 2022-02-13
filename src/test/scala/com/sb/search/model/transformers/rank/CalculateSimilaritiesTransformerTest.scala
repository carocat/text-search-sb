package com.sb.search.model.transformers.rank

import com.sb.search.model.SparkBaseTest
import com.sb.search.model.data.RankWordsTable
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.DataFrame

class CalculateSimilaritiesTransformerTest extends SparkBaseTest {

  test("testHighSimilarity") {
    val result = new CalculateSimilaritiesTransformer(getTextSearchHighSimilarityDs).transform(getCatalogueSearchDs)
    assert(0.9999999999999999 == result.select(RankWordsTable.SIMILARITY).collectAsList().get(0).getDouble(0))
  }

  test("testLowSimilarity") {
    val result = new CalculateSimilaritiesTransformer(getTextSearchLowSimilarityDs).transform(getCatalogueSearchDs)
    assert(0.07016464154456234 == result.select(RankWordsTable.SIMILARITY).collectAsList().get(0).getDouble(0))
  }

  //scalastyle:off
  private def getTextSearchHighSimilarityDs: DataFrame = {
    val data = Array(
      Vectors.sparse(5, Seq((1, 1.0), (3, 7.0)))
    )
    sparkSession.sqlContext.createDataFrame(data.map(Tuple1.apply)).toDF(RankWordsTable.FEATURES_SEARCH)
  }

  //scalastyle:off
  private def getTextSearchLowSimilarityDs: DataFrame = {
    val data = Array(
      Vectors.sparse(10, Seq((1, 4.0), (2, 7.0)))
    )
    sparkSession.sqlContext.createDataFrame(data.map(Tuple1.apply)).toDF(RankWordsTable.FEATURES_SEARCH)
  }

  //scalastyle:off
  private def getCatalogueSearchDs: DataFrame = {
    val data = Array(
      Vectors.sparse(5, Seq((1, 1.0), (3, 7.0)))
    )
    sparkSession.sqlContext.createDataFrame(data.map(Tuple1.apply)).toDF(RankWordsTable.FEATURES)
  }
}

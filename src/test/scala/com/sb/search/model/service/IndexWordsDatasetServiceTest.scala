package com.sb.search.model.service

import com.sb.search.model.SparkBaseTest
import com.sb.search.model.data.{CatalogueTable, IndexWordsTable}
import com.sb.search.model.input.common.utils.DatasetHelperService
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, Dataset, Row}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.mockito.Mockito.{mock, when}

class IndexWordsDatasetServiceTest extends SparkBaseTest {

  val datasetHelperServiceMock: DatasetHelperService = mock(classOf[DatasetHelperService])
  val indexWordsDatasetService = new IndexWordsDatasetService(datasetHelperServiceMock)


  test("testRunIndexWordsDatasetService") {
    when(datasetHelperServiceMock.getCatalogue).thenReturn(getCatalogue)
    indexWordsDatasetService.run()
  }

  test("testGetIndexWordsDataset") {
    when(datasetHelperServiceMock.getCatalogue).thenReturn(getCatalogue)
    val result = indexWordsDatasetService.getIndexWordsDataset(getCatalogue)
    assertDatasetEquals(getExpectedDs.select(testColumns:_*), result.transform(getCatalogue).select(testColumns:_*))
  }


  //scalastyle:off
  private def getCatalogue: Dataset[Row] = {
    sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(
        Seq(
          Row(1, "These cases are perfectly simple and easy to distinguish"),
          Row(2, "simple are perfectly cases"),
          Row(3, "cases are perfectly simple")
        )
      ),
      StructType(
        getStructDs
      )
    )
  }

  //scalastyle:off
  private def getStructDs: StructType = StructType(Array[StructField](
    StructField(CatalogueTable.ID, DataTypes.IntegerType, nullable = true),
    StructField(CatalogueTable.TEXT, DataTypes.StringType, nullable = true)
  ))


  //scalastyle:off
  private def getExpectedDs: Dataset[Row] = {
    sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(
        Seq(
          Row(1, "These cases are perfectly simple and easy to distinguish", List("cases", "perfectly", "simple", "easy", "distinguish")),
          Row(2, "simple are perfectly cases", List("simple", "perfectly", "cases")),
          Row(3, "cases are perfectly simple", List("cases", "perfectly", "simple"))
        )
      ),
      StructType(
        getExpectedStructDs
      )
    )
  }

  //scalastyle:off
  private def getExpectedStructDs: StructType = StructType(Array[StructField](
    StructField(IndexWordsTable.ID, DataTypes.IntegerType, nullable = true),
    StructField(IndexWordsTable.TEXT, DataTypes.StringType, nullable = true),
    StructField(IndexWordsTable.FILTERED, DataTypes.createArrayType(DataTypes.StringType), nullable = true)
  ))

  private def testColumns = Seq[Column](
    col(IndexWordsTable.ID),
    col(IndexWordsTable.FILTERED)
  )

}

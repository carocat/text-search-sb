package com.sb.search.model.transformers.rank

import com.sb.search.model.SparkBaseTest
import com.sb.search.model.data.RankWordsTable
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row}

class RankTransformerTest extends SparkBaseTest {

  test("testRank") {
    val result = new RankTransformer().transform(getDs)
    assertDatasetEquals(getExpectedDs, result)
  }

  //scalastyle:off
  private def getDs: Dataset[Row] = {
    sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(
        Seq(
          Row(1, 0.002d),
          Row(2, 0.2d),
          Row(3, 0.3d),
          Row(4, 0.4d),
          Row(5, 0.5d),
          Row(6, 0.6d),
          Row(7, 0.7d),
          Row(8, 0.8d),
          Row(9, 0.9d),
          Row(10, 0.72d),
          Row(11, 0.65d)
        )
      ),
      StructType(
        getStructDs
      )
    )
  }

  //scalastyle:off
  private def getStructDs: StructType = StructType(Array[StructField](
    StructField(RankWordsTable.ID, DataTypes.IntegerType, nullable = true),
    StructField(RankWordsTable.SIMILARITY, DataTypes.DoubleType, nullable = true)
  ))

  //scalastyle:off
  private def getExpectedDs: Dataset[Row] = {
    sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(
        Seq(
          Row(9, 0.9d, 1),
          Row(8, 0.8d, 2),
          Row(10, 0.72d, 3),
          Row(7, 0.7d, 4),
          Row(11, 0.65d, 5),
          Row(6, 0.6d, 6),
          Row(5, 0.5d, 7),
          Row(4, 0.4d, 8),
          Row(3, 0.3d, 9),
          Row(2, 0.2d, 10)
        )
      ),
      StructType(
        getStructDs
      )
        .add(
          StructField(
            RankWordsTable.RANK, DataTypes.IntegerType, nullable = true)
        )
    )
  }
}

package com.sb.search.model.transformers.rank

import com.sb.search.model.data.RankWordsTable
import com.sb.search.model.transformers.{CopyTransformer, SchemaTransformer, UuidTransformer}
import org.apache.spark.ml.Transformer
import org.apache.spark.sql.functions.{col, udf, when}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.ml.linalg.Vector

class CalculateSimilaritiesTransformer(textToSearch: Dataset[Row]) extends Transformer with CopyTransformer with UuidTransformer with SchemaTransformer {
  override def transform(dataset: Dataset[_]): DataFrame = {
    dataset.crossJoin(textToSearch)
      .withColumn(RankWordsTable.SIMILARITY, cosSimilarity(col(RankWordsTable.FEATURES), col(RankWordsTable.FEATURES_SEARCH)))
      .withColumn(RankWordsTable.SIMILARITY, when(col(RankWordsTable.SIMILARITY).isNaN, 0).otherwise(col(RankWordsTable.SIMILARITY)))
  }

  val cosSimilarity = udf { (x: Vector, y: Vector) =>
    val v1 = x.toArray
    val v2 = y.toArray
    val l1 = scala.math.sqrt(v1.map(x => x * x).sum)
    val l2 = scala.math.sqrt(v2.map(x => x * x).sum)
    val scalar = v1.zip(v2).map(p => p._1 * p._2).sum
    scalar / (l1 * l2)
  }

}


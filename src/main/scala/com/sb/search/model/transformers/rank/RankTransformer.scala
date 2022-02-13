package com.sb.search.model.transformers.rank

import com.sb.search.model.config.configuration
import com.sb.search.model.data.RankWordsTable
import com.sb.search.model.transformers.{CopyTransformer, SchemaTransformer, UuidTransformer}
import org.apache.spark.ml.Transformer
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}
import org.apache.spark.sql.{DataFrame, Dataset}

class RankTransformer extends Transformer with CopyTransformer with UuidTransformer with SchemaTransformer {
  override def transform(dataset: Dataset[_]): DataFrame = {
    val windowSpec  = Window.orderBy(col(RankWordsTable.SIMILARITY).desc)
    dataset
      .withColumn(RankWordsTable.RANK, row_number().over(windowSpec))
      .filter(col(RankWordsTable.RANK)between(1, configuration.rankWordsMetrics.rankSize))

  }
}

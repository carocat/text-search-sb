package com.sb.search.model.transformers

import org.apache.spark.ml.Transformer
import org.apache.spark.sql.{DataFrame, Dataset}

class LoggerTransformer extends Transformer with CopyTransformer with UuidTransformer with SchemaTransformer {



  override def transform(dataset: Dataset[_]): DataFrame = {

    dataset.show()
    dataset.toDF()

  }

}

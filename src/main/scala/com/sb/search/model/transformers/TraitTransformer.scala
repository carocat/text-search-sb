package com.sb.search.model.transformers

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.types.StructType

import java.util.UUID

trait CopyTransformer extends Transformer {
  override def copy(extra: ParamMap): Transformer = this
}

trait SchemaTransformer extends Transformer {
  override def transformSchema(schema: StructType): StructType = schema
}

trait UuidTransformer extends Transformer {
  override val uid: String = UUID.randomUUID().toString

}

package com.sb.search.model

package object config {

  import pureconfig.generic.auto._
  import pureconfig.configurable._
  import pureconfig.ConfigSource.default.loadOrThrow

  val configuration: RootConfig = loadOrThrow[RootConfig]

}

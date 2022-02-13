package com.sb.search.model

import com.sb.search.model.config.configuration
import com.sb.search.model.input.common.actions.AllActions
import com.sb.search.model.input.common.utils.{ConstantsAction, ConstantsService}
import org.apache.log4j.{Level, LogManager, Logger}

class EntryPoint

object EntryPoint {

  Logger.getLogger(getClass.getName).setLevel(Level.INFO)
  private val log = LogManager.getLogger(getClass.getName)

  private val allActions = AllActions


  def main(args: Array[String]): Unit = {

    val action = System.getProperty(ConstantsService.ACTION)

    log info s" action is: $action"

    allActions
      .buildAction(matchAction(action))
      .run()
  }

  def matchAction(x: String): String = x match {

    case ConstantsAction.ACTION_INDEX_WORDS => configuration.allActionsClass.indexWordsDataset
    case ConstantsAction.RANK_WORDS => configuration.allActionsClass.rankWordsDataset
    case _ => throw new Exception("action doesn't exist")

  }
}

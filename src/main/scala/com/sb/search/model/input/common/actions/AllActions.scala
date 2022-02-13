package com.sb.search.model.input.common.actions

import com.sb.search.model.GuiceInit
import com.sb.search.model.input.common.utils.FunctionalInterfaceRunService
import org.apache.log4j.{Level, LogManager, Logger}

object AllActions {

  Logger.getLogger(getClass.getName).setLevel(Level.INFO)
  private val log = LogManager.getLogger(getClass.getName)

  def buildAction(className: String): FunctionalInterfaceRunService = {

    val classFromName = Class.forName(className)
    log info s"class name is $classFromName"
    val injector = new GuiceInit(classFromName.getSimpleName).getInjector
    val interfaceRunService = injector
      .getInstance(classFromName.asSubclass(classOf[FunctionalInterfaceRunService]))
    interfaceRunService
  }

}

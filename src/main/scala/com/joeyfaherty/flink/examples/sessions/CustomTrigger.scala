package com.joeyfaherty.flink.examples.sessions

import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.Window

class CustomTrigger[W <: Window] extends Trigger[Transaction, W] {

  /*
   * When a txType of "win" is received that session is closed, otherwise the stream continues
   */
  override def onElement(element: Transaction, timestamp: Long, window: W, ctx: Trigger.TriggerContext): TriggerResult = {
    if (element.txType.isDefined) {
      if (element.txType.get == "closeSession") {
        TriggerResult.FIRE
      }
      else {
        TriggerResult.CONTINUE
      }
    }
    else {
      TriggerResult.CONTINUE
    }
  }

  override def onEventTime(time: Long, window: W, ctx: Trigger.TriggerContext): TriggerResult = {
    // fire the trigger and delete all window contents
    TriggerResult.FIRE_AND_PURGE
  }


  override def onProcessingTime(time: Long, window: W, ctx: Trigger.TriggerContext): TriggerResult = {
    // ignore processing time triggers, we never set them
    TriggerResult.CONTINUE
  }

  override def clear(window: W, ctx: Trigger.TriggerContext): Unit = {
    // purge the window
    TriggerResult.PURGE
  }
}
package com.joeyfaherty.flink.examples.sessions

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow

import scala.util.Try

case class Transaction(sessionId: String, txAmount: Double, txType: Option[String])

/**
  *
  * Run the main function below
  *
  * nc -lk 5555 (from terminal)
  *
  * Input:
  *
  * session1,20
  * session1,20
  * session1,20,closeSession
  *
  */
object TransactionSession {

  def main(args: Array[String]): Unit = {

    // get the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // get input data by creating a stream from socket
    val input = env.socketTextStream("localhost", 5555)

    // ensure to import implicits to avoid strange compilation errors
    import org.apache.flink.streaming.api.scala._

    val values = input
      .map(v => {
        val cols = v.split(",")
        val sessionId = cols(0)
        val txAmount = cols(1).toDouble
        val closeSignal = Try(Some(cols(2))).getOrElse(None)

        Transaction(sessionId, txAmount, closeSignal)
      })

    // create keyedstream stream based on session id
    val kv = values.keyBy(_.sessionId)

    // this WindowedStream will track the sessions
    val sessionWindowStream: WindowedStream[Transaction, String, GlobalWindow] = kv.
      window(GlobalWindows.create())
      // global window is our window assigner
      // we use a purging trigger to ensure that the session is purged once it is evaluated
      .trigger(PurgingTrigger.of(new CustomTrigger[GlobalWindow]()))


    sessionWindowStream.sum("txAmount").print()

    env.execute("Custom Session Window")
  }
}
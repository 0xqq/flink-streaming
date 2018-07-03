package com.joeyfaherty.flink.examples.windows

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/*
 * Count based window evaluates when number of events hits the threshold defined.
 */
object FlinkSocketWordCountCountBasedWindow {

  def main(args: Array[String]): Unit = {

    // get the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // get input data by creating a stream from socket
    val input = env.socketTextStream("localhost", 5555)

    // ensure to import implicits to avoid strange compilation errors
    import org.apache.flink.streaming.api.scala._

    // parse the data, group it, window it, and aggregate the counts
    val words = input
      .flatMap(line => line.split("\\s"))
      .map(word => (word, 1))

    val keyValue = words.keyBy(0)
      // count window of 15 that evaluates when number of events hits the threshold defined.
      .countWindow(15)

    val wordCount = keyValue.sum(1)

    wordCount.print()

    env.execute("Simple Word Count")
  }

}

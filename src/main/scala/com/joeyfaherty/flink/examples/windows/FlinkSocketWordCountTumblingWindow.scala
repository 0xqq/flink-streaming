package com.joeyfaherty.flink.examples.windows

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/*
 * Tumbling window is a fixed size and is comparable to Spark's micro-batch system.
 */
object FlinkSocketWordCountTumblingWindow {

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
      // split into tumbling windows of 5 seconds
      .timeWindow(Time.seconds(5))


    val wordCount = keyValue.sum(1)


    wordCount.print()

    env.execute("Simple Word Count")
  }

}

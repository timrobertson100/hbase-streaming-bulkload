package com.opencore.sample

import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * A very simple example which will connect to a socket on localhost port 7001 and bulkput to a local HBase every
  * 10 secs with a new row for every line of input read.
  */
object BulkLoad {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Bulkload example")
    conf.setIfMissing("spark.master", "local[2]")
    val sc = new SparkContext(conf)
    try {

      // For the simplest example we connect to a socket on port 7001 and then micro batch content at 10 second intervals
      val ssc = new StreamingContext(sc, Seconds(10))
      val input = ssc.socketTextStream("localhost", 7001)

      // There will be a streaming bulk put issued every 10 seconds
      val hbaseContext = new HBaseContext(sc, HBaseConfiguration.create())
      hbaseContext.streamBulkPut[String](
        input,
        TableName.valueOf("streamingtest"),
        (record) => {

          if (record.length() > 0) { // defensive coding only

            val put: Put = new Put(Bytes.toBytes(record))
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("id"), Bytes.toBytes(record))

          } else {
            null
          }
        }
      )

      ssc.start()
      ssc.awaitTerminationOrTimeout(60000) // run the demo for 1 minute

    } finally {
      sc.stop()
    }
  }
}

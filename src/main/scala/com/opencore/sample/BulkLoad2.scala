package com.opencore.sample

import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.{Map => MMap}

/**
  * A slightly modified version of the Bulkload just demonstrating that the DStream can have more complex objects than
  * a simple String.
  *
  * Other than adding complexity, this demonstrates nothing beyond the Bulkload example.
  *
  * Here the first word of the line becomes the HBase rowKey, while the remainder become columns c1,c2,c3 etc.
  */
object BulkLoad2 {

  // Wrapper around a Mutable Map to simulate a row containing cells
  class HBaseRow {
    private val data = MMap[String, String]()

    def setRowKey(key: String) = data.put("index", key)
    def getRowKey() = Bytes.toBytes(data.get("index").get)
    def put(column: String, value: String) = data.put(column, value)
    def get(column: String) = data.get(column)
    def getColumns() = data.filter(e => e._1!="index") // strip the index from the columns
    def getColumnFamily() = "c"
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Bulkload example")
    conf.setIfMissing("spark.master", "local[2]")
    val sc = new SparkContext(conf)
    try {

      // For the simplest example we connect to a socket on port 7001 and then micro batch content at 10 second intervals
      val ssc = new StreamingContext(sc, Seconds(10))
      val input = ssc.socketTextStream("localhost", 7001)

      // Convert the stream into a stream simulating HBaseRow objects
      // This will split the line text into words, and the first word becomes the key
      val indexRows : DStream[HBaseRow] = input.map(line => {
        var row = new HBaseRow
        // first column becomes the key, the rest just cells
        line.split(" ").zipWithIndex.foreach{
          case(word,i) => {
            if (i==0) row.setRowKey(word)
            else row.put("c"+i, word)
          }
        }
        row
      })

      // There will be a streaming bulk put issued every 10 seconds
      val hbaseContext = new HBaseContext(sc, HBaseConfiguration.create())
      hbaseContext.streamBulkPut[HBaseRow](
        indexRows,
        TableName.valueOf("streamingtest"),
        (indexRow: HBaseRow) => {

          val put: Put = new Put(indexRow.getRowKey)

          // create a column per word
          indexRow.getColumns.map{case(columnName, value) =>
            put.addColumn(indexRow.getColumnFamily.getBytes(), Bytes.toBytes(columnName), Bytes.toBytes(value))
          }

          put
        }
      )

      ssc.start()
      ssc.awaitTerminationOrTimeout(60000) // run the demo for 1 minute

    } finally {
      sc.stop()
    }
  }
}

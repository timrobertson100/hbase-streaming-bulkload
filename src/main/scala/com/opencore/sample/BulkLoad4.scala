package com.opencore.sample

import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._

/**
  * A version of the Bulkload demonstrating that the DStream can use the HBaseRow container Java object reading from
  * Kafka.
  * Here the first word of the kafka message becomes the HBase rowKey, while the remainder become columns c1,c2,c3 etc.
  */
object BulkLoad4 {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Bulkload example")
    conf.setIfMissing("spark.master", "local[2]")
    val sc = new SparkContext(conf)
    try {

      val ssc = new StreamingContext(sc, Seconds(10))

      val kafkaParams = Map[String, String](
        "metadata.broker.list" -> "localhost:9092",
        "group.id" -> "test"
      )

      val topicSet = Set("test")

      val input = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet)

      // Convert the stream into a stream simulating HBaseRow objects
      // This will split the line text into words, and the first word becomes the key
      val indexRows : DStream[HBaseRow] = input.map(message => {
        var row = new HBaseRow
        row.setColumnFamily("c")

        // first column becomes the key, the rest just cells
        message._2.split(" ").zipWithIndex.foreach{
          case(word,i) => {
            if (i==0) row.setRowKey(word.getBytes())
            else row.getColumns.put("c"+i, word.getBytes())
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

          indexRow.getColumns.asScala.map{ case(columnName, value) => {
            put.addColumn(indexRow.getColumnFamily.getBytes(), columnName.getBytes, value)
          }}

          put
        }
      )

      ssc.start()
      ssc.awaitTerminationOrTimeout(600000) // run the demo for 10 minutes

    } finally {
      sc.stop()
    }
  }
}

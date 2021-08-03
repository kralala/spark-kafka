package one

import one.Kafka
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Properties
import org.apache.commons.lang3.ArrayUtils
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketReceiver
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Durations
import org.apache.spark.streaming.api.java.JavaDStream
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.apache.log4j.Logger

import java.util.Arrays

object Application{

  val max = 1073741824
  val min = 1024
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  def main(args: Array[String]): Unit = {
    val c: SparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("traffic")

    val jsc: JavaStreamingContext = new JavaStreamingContext(c, Durations.seconds(300))

    Logger.getRootLogger.setLevel(Level.WARN)

    val producer = new KafkaProducer[String, String](props)

    val jdstream: JavaDStream[java.lang.Long] = jsc
      .receiverStream(new Kafka())
      //.map(packet => ArrayUtils.toObject(packet.getRawData()))
      .count()

    jdstream.print();
    jdstream.foreachRDD(rdd => {
      if (rdd.count() < min || rdd.count() > max) {
        producer.send(new ProducerRecord("Alerts", "Traffic Exceeding"));
        producer.close()
      }
    })

    jsc.start()
    jsc.awaitTermination()

  }
}

package grasselli.scala_kafka

import com.github.nscala_time.time.Imports._
import java.util.Properties
import kafka.consumer._
import kafka.api._
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

class PartitionReader(brokers: List[String], port: Integer) {
  val example = new SimpleExample();

  def findOffset(topic: String, partition: Integer, time: DateTime): Long = {
    val timestamp : Long = time.getMillis / 1000

    @annotation.tailrec
    def x(offset_1: Long, offset_2: Long): Long = {
      val first = getTimestampAndOffset(offset_1, topic, partition)
      val last = getTimestampAndOffset(offset_2, topic, partition)
      val new_offset = (first._2 + last._2) / 2
      val middle = getTimestampAndOffset(new_offset, topic, partition)

      if(last._2 - first._2 == 1) {
        println(first._1)
        first._2
      } else if(timestamp > middle._1) {
        x(middle._2, last._2)
      } else {
        x(first._2, middle._2)
      }
    }

    x(-2L, -1L)
  }

  def getTimestampAndOffset(offset: Long, topic: String, partition: Integer): Tuple2[Long,Long] = {
    val java_brokers: java.util.List[String] = ListBuffer(brokers: _*)

    val fetch_response = example.fetch(topic, partition, java_brokers, port, offset)

    var time = 0L
    var new_offset = 0L

    for (message <- fetch_response.messageSet(topic, partition)) {
      if (time == 0) {
        time = MessagePackSerialization.unpack(message)("timestamp").asInstanceOf[Number].longValue
        new_offset = message.offset
      }

    }

    (time, new_offset)
  }
}


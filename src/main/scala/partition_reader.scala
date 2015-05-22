package grasselli.scala_kafka

import com.github.nscala_time.time.Imports._
import java.util.Properties
import kafka.consumer._
import kafka.api._
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

class PartitionReader(brokers: List[String], port: Integer) {
  val example = new SimpleExample();

  def findOffset(topic: String, partition: Integer, time: DateTime): Any = {
    /* val timestamp = asd */
    val result_1 = getTimestampAndOffset(-2L, topic, partition)
    val result_2 = getTimestampAndOffset(-1L, topic, partition)

    result_1._2
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


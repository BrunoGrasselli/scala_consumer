package grasselli.scala_kafka

import kafka.consumer._
import kafka.serializer._
import kafka.utils._
import java.util.Properties
import kafka.utils.Logging
import scala.collection.JavaConversions._

class Config(args: Array[String]) {
  val config_string = args

  def topic() : String = {
    if(config_string.length > 0) config_string(0) else "tracking"
  }

  def host() : String = {
    if(config_string.length > 1) config_string(1) else "localhost:2181"
  }

  def maxMessages() : Int = {
    if(config_string.length > 2) config_string(2).toInt else 0
  }

  def filterKey() : Option[String] = {
    if(config_string.length > 3) Some(config_string(3)) else None
  }

  def filterValue() : Option[String] = {
    if(config_string.length > 4) Some(config_string(4)) else None
  }

  def groupId() : String = {
    "consumer_123"
  }

  def isFiltering() : Boolean = {
    !filterKey.isEmpty && !filterValue.isEmpty
  }
}


object Consumer {

  def main(args: Array[String]): Unit = {
    val config = new Config(args)

    println(s"Topic: ${config.topic}, Zookeeper host: ${config.host}")

    val props = new Properties()

    ZkUtils.maybeDeletePath(config.host, s"/consumers/${config.groupId}");

    props.put("group.id", config.groupId)
    props.put("zookeeper.connect", config.host)
    props.put("auto.offset.reset", "largest")
    props.put("auto.commit.enable", "false")

    val consumer_config = new ConsumerConfig(props)

    val connector = kafka.consumer.Consumer.create(consumer_config)

    val filterSpec = new Whitelist(config.topic)

    val stream = connector.createMessageStreamsByFilter(filterSpec, 1, new DefaultDecoder(), new DefaultDecoder()).get(0)

    var message_count = 0
    var message_filter_count = 0
    var running = true

    for (messageAndMetadata <- stream) {
      message_count = message_count + 1

      if (config.maxMessages > 0 && message_count > config.maxMessages) {
        connector.shutdown()
      }
      else {
        val message = MessagePackSerialization.unpack(messageAndMetadata)

        if (config.isFiltering)  {
          if (message.get(config.filterKey.get).get == config.filterValue.get) {
            message_filter_count = message_filter_count + 1
            println(message + " | Message count -> " + {message_filter_count})
          }
        }
        else {
          println(message)
        }
      }
    }
  }
}

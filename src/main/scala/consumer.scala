package grasselli.scala_kafka

import kafka.message._
import kafka.consumer._
import kafka.serializer._
import kafka.utils._
import java.util.Properties
import kafka.utils.Logging
import scala.collection.JavaConversions._
import org.msgpack.MessagePack
import scala.collection.mutable.HashMap

object Consumer {
  def unpack(messageAndMetadata: MessageAndMetadata[Array[Byte], Array[Byte]]) = {
    val unpackedMessage: HashMap[String, Any] = new HashMap

    val unpacked = MessagePack.unpack(messageAndMetadata.message)
    for (entry <- unpacked.asMapValue.entrySet) {
      val value: Any = entry.getValue match {
        case v if v.isBooleanValue => v.asBooleanValue
        case v if v.isIntegerValue => v.asIntegerValue
        case v if v.isFloatValue => v.asFloatValue
        case v if v.isRawValue => v.asRawValue.getString
        case v => None
      }
      unpackedMessage.put(entry.getKey.asRawValue.getString, value)
    }

    unpackedMessage
  }

  def main(args: Array[String]) {
    val topic = if(args.length > 0) args(0) else "tracking"
    val zookeeper = if(args.length > 1) args(1) else "localhost:2181"

    println(s"Topic: ${topic}, Zookeeper: ${zookeeper}")

    val props = new Properties()

    props.put("group.id", "bruno_consumer_1234")
    props.put("zookeeper.connect", zookeeper)
    props.put("auto.offset.reset", "largest")

    val config = new ConsumerConfig(props)

    val connector = kafka.consumer.Consumer.create(config)

    val filterSpec = new Whitelist(topic)

    val stream = connector.createMessageStreamsByFilter(filterSpec, 1, new DefaultDecoder(), new DefaultDecoder()).get(0)

    for (messageAndMetadata <- stream) {
      println(unpack(messageAndMetadata))
    }
  }
}

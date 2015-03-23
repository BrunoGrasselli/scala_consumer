package grasselli.scala_kafka

import kafka.consumer._
import kafka.serializer._
import kafka.utils._
import java.util.Properties
import kafka.utils.Logging
import scala.collection.JavaConversions._

object Consumer {

  def main(args: Array[String]): Unit = {
    val topic = if(args.length > 0) args(0) else "tracking"
    val zookeeper = if(args.length > 1) args(1) else "localhost:2181"
    val group_id = "bruno_consumer_1234"

    println(s"Topic: ${topic}, Zookeeper: ${zookeeper}")

    val props = new Properties()

    ZkUtils.maybeDeletePath(zookeeper, s"/consumers/${group_id}");

    props.put("group.id", group_id)
    props.put("zookeeper.connect", zookeeper)
    props.put("auto.offset.reset", "largest")

    val config = new ConsumerConfig(props)

    val connector = kafka.consumer.Consumer.create(config)

    val filterSpec = new Whitelist(topic)

    val stream = connector.createMessageStreamsByFilter(filterSpec, 1, new DefaultDecoder(), new DefaultDecoder()).get(0)

    for (messageAndMetadata <- stream) {
      println(MessagePackSerialization.unpack(messageAndMetadata))
    }
  }
}

package grasselli.scala_kafka
import scala.collection.mutable.HashMap
import org.msgpack.MessagePack
import kafka.message._
import scala.collection.JavaConversions._

object MessagePackSerialization {
  val messagePack = new MessagePack()

  def unpack(messageAndMetadata: MessageAndMetadata[Array[Byte], Array[Byte]]): HashMap[String, Any] = {
    val unpackedMessage: HashMap[String, Any] = new HashMap

    val unpacked = messagePack.read(messageAndMetadata.message)

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

}

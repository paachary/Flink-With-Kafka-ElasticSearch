package flink

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * Customized kafkaDeserializerschema class which is used to decode the kafka message into its various
 * attributes
 */
class CustomerDeserializer extends KafkaDeserializationSchema [String]{

  override def isEndOfStream(nextElement: String): Boolean = false

  /**
   * The function decodes the messages into
   * Key, Value, Timestamp, Offset and Partition
   * @param record -> the message from the kafka topic
   * @return
   */
  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): String = {

   val data : String = new String(record.key())+"~"+
      new String(record.value())+"~"+
      record.timestamp().toString+"~"+
      record.offset().toString+"~"+
      record.partition().toString
    data
  }

  override def getProducedType: TypeInformation[String] =
    TypeInformation.of(classOf[String])
}

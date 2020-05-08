package flink

import java.util.Properties

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
 * Flink streaming consumer which consumers messages from a kafka topic and populates an elasticsearch index
 */
object FlinkConsumer {

  /**
   * The orchestrator method which initiates the connection to Kafka broker and consumes the data from a kafka topic.
   * The function then initiates a connection with an elasticsearch index and populates the data into the index.
   * @param args command line arguments. If nulls are passed, they default to known values.
   */
  def main(args: Array[String]) : Unit = {

    if (args.length != 2){
      println("Please pass the right number of arguments to the program\n" +
        "\t Usage >> \t FlinkConsumer <topic name> <elasticsearch index name> ")
    }else{

      var topicName  : String = args(0)

      if ( topicName.trim.length == 0 )
        topicName = "twitter_topic"

      var esIndexName : String = args(1)
      if (esIndexName.trim.length == 0 )
        esIndexName = "twitter"

      val localProperties : Properties = new Properties()

      localProperties.setProperty("indexName", esIndexName)
      localProperties.setProperty("topic", topicName)

      localProperties.setProperty("kafkabootstrapServers", "localhost:9092")
      localProperties.setProperty("consumerGroupId", "first_group_id")

      val properties = new Properties ()
      properties.setProperty ("bootstrap.servers", localProperties.getProperty("kafkabootstrapServers"))
      properties.setProperty ("group.id", localProperties.getProperty("consumerGroupId"))

      val env = StreamExecutionEnvironment.getExecutionEnvironment

      val consumer :FlinkKafkaConsumer[String] =
        new FlinkKafkaConsumer[String] (topicName, new CustomerDeserializer(), properties)

      val dataStream : DataStream[String] = env.addSource(consumer)

      val elasticSearchConsumer = new ElasticSearchConsumer()

      dataStream.addSink(elasticSearchConsumer.esSinkBuilder.build())

      env.execute ("Flink Scala Kafka Invocation")
    }
  }
}


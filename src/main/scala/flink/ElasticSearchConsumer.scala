package flink

import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch7.{ElasticsearchSink, RestClientFactory}
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.{Requests, RestClientBuilder}

/**
 * ElasticSearch class which acts as a sink for the Kafka topic messages
 */
class ElasticSearchConsumer()
{
  val hostname : String = "localhost"
  val port : Int =  9200
  val scheme : String = "http"

  val httpHosts = new java.util.ArrayList[HttpHost]

  httpHosts.add(new HttpHost(hostname, port, scheme))

  val esSinkBuilder: ElasticsearchSink.Builder[String] =
    new ElasticsearchSink.Builder[String](httpHosts, new ElasticsearchSinkFunction[String]{

      /**
       * This is a customized version of the overridden method in the ElasticsearchSinkFunction
       * which decodes the topic data and loads into the elasticsearch index name
       * @param t -> the data coming from the kafka topic
       * @param runtimeContext -> runtime context
       * @param requestIndexer -> requestIndex of the elasticsearch
       */
      override def process(t: String, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {

        val field : Array[String] =  t.split("~")

        val key = field(0)
        val value = field(1)
        val timestamp = field(2)
        val offset = field(3)
        val partition = field(4)

        val json = new util.HashMap[String, String]()

        json.put("id", key)
        json.put("value", value)
        json.put("timestamp", timestamp)
        json.put("offset", offset)
        json.put("partition", partition)

        val esIndexName : String = "twitter"

        val requestIndex : IndexRequest = Requests.indexRequest()
        .index(esIndexName)
        .id(key)
        .source(json)

      requestIndexer.add(requestIndex)

    }
  }
  )
 // configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
  esSinkBuilder.setBulkFlushMaxActions(1)
}

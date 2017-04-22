package org.cmatta.kafka.streams.wikipedia;

import io.confluent.examples.streams.utils.SpecificAvroDeserializer;
import io.confluent.examples.streams.utils.SpecificAvroSerde;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.log4j.Logger;
import org.cmatta.kafka.connect.irc.Message;

import java.util.Properties;

/**
 * Created by chris on 10/15/16.
 */
public class MessageMonitor {
  private static final Logger log = Logger.getLogger(MessageMonitor.class);

  public static void main(String[] args) throws Exception {
    Properties props = new Properties();

    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wikipedia-monitor");

    if(System.getenv("CHANGESMONITOR_BOOTSTRAP_SERVERS") == null ||
        System.getenv("CHANGESMONITOR_BOOTSTRAP_SERVERS").equals("")) {
      log.warn("No environment variable CHANGESMONITOR_BOOTSTRAP_SERVERS set, defaulting to 'kafka:9092'");
      props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
    } else {
      props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("CHANGESMONITOR_BOOTSTRAP_SERVERS"));
    }

    if(System.getenv("CHANGESMONITOR_SCHEMA_REGISTRY_URL") == null ||
        System.getenv("CHANGESMONITOR_SCHEMA_REGISTRY_URL").equals("")) {
      log.warn("No environment variable CHANGESMONITOR_SCHEMA_REGISTRY_URL set, defaulting to 'http://schemaregistry:8081'");
      props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://schemaregistry:8081");
    } else {
      props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, System.getenv("CHANGESMONITOR_SCHEMA_REGISTRY_URL"));
    }


    props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put("consumer.interceptor.classes", "io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor");
    props.put("producer.interceptor.classes", "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor");

    KStreamBuilder builder = new KStreamBuilder();

    KStream<String, Message> wikipediaRaw = builder.stream("wikipedia.raw");

    wikipediaRaw.map(WikipediaMessageParser::parseMessage)
        .filter((k, v) -> k != null && v != null).to("wikipedia.parsed");

//    TODO Add KTable example code
    final KafkaStreams streams = new KafkaStreams(builder, props);
    streams.start();

    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));


  }
}

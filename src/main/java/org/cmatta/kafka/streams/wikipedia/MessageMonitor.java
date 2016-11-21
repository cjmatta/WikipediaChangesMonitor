package org.cmatta.kafka.streams.wikipedia;

import io.confluent.examples.streams.utils.SpecificAvroSerde;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import org.cmatta.kafka.connect.irc.Message;
import org.cmatta.kafka.streams.wikipedia.avro.WikipediaChange;

import java.util.Properties;

/**
 * Created by chris on 10/15/16.
 */
public class MessageMonitor {
  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wikipedia-monitor");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
    props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "zookeeper:2181");
    props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://schemaregistry:8081");
    props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put("consumer.interceptor.classes", "io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor");
    props.put("producer.interceptor.classes", "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor");

    KStreamBuilder builder = new KStreamBuilder();

    KStream<String, Message> wikipediaRaw = builder.stream("wikipedia.raw");

    KStream<String, WikipediaChange> wikipediaParsed = wikipediaRaw.map(WikipediaMessageParser::parseMessage)
        .filter((k, v) -> k != null && v != null);

    wikipediaParsed.to("wikipedia.parsed");

//    Top Editors
//    KTable<Windowed<String>, Long> editorCounts = wikipediaParsed.mapValues(v -> {
//          return new KeyValue<>(v.getUsername(), 1);
//        }).groupByKey().count(TimeWindows.of(60*60*24*1000L).advanceBy(60*1000L), "EditorCountsStore");
//
//    final KafkaStreams streams = new KafkaStreams(builder, props);
//    streams.start();

    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));


  }
}

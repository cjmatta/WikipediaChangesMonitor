package org.cmatta.kafka.streams.wikipedia;

import io.confluent.examples.streams.utils.SpecificAvroSerde;
import io.confluent.examples.streams.utils.WindowedSerde;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.cmatta.kafka.connect.irc.Message;
import org.cmatta.kafka.streams.wikipedia.avro.WikipediaChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created by chris on 11/21/16.
 */
public class InteractiveQueries {
  static final int REST_PROXY_PORT = 8080;
  static final Logger log = LoggerFactory.getLogger(InteractiveQueries.class);

  public static void main(String[] args) throws Exception {
    Properties streamsProps = new Properties();
    streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wikipedia-monitor-nemo");
    streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
    streamsProps.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "zookeeper:2181");
    streamsProps.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://schemaregistry:8081");
    streamsProps.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamsProps.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
    streamsProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    streamsProps.put("consumer.interceptor.classes", "io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor");
    streamsProps.put("producer.interceptor.classes", "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor");

    final KafkaStreams streams = createStreams(streamsProps);
    streams.start();

    final WikipediaRestService restService = startRestProxy(streams, REST_PROXY_PORT);

    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        streams.close();
        restService.stop();
      } catch (Exception e) {
        log.error(e.getMessage());
      }
    }));
  }

  static WikipediaRestService startRestProxy(final KafkaStreams streams, final int port) throws Exception{
    final WikipediaRestService wikipediaRestService = new WikipediaRestService(streams);
    wikipediaRestService.start(port);
    return wikipediaRestService;
  }

  static KafkaStreams createStreams(final Properties streamsConfiguration) {
    final Serde<String> stringSerde = Serdes.String();
    final Serde<Long> longSerde = Serdes.Long();
    final Serde<Windowed<String>> windowedStringSerde = new WindowedSerde<>(stringSerde);

    KStreamBuilder builder = new KStreamBuilder();

    KStream<String, WikipediaChange> wikipediaParsed = builder.stream("wikipedia.parsed");

    //    Top Editors
    KTable<Windowed<String>, Long> editorCounts = wikipediaParsed.map((k, v) -> {
      return new KeyValue<>(v.getUsername(), 1L);
    }).groupByKey(stringSerde, longSerde).count(TimeWindows.of(60 * 60 * 1000L), "EditorCounts");


    return new KafkaStreams(builder, streamsConfiguration);
  }
}

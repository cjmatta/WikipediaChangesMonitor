package org.cmatta.kafka.streams.wikipedia;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

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


    final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
    final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
    final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

    KStreamBuilder builder = new KStreamBuilder();

    KStream<JsonNode, JsonNode> wikipediaRaw = builder.stream(jsonSerde, jsonSerde, "wikipedia");
    wikipediaRaw.map(WikipediaMessageParser::getRawMessage).to(Serdes.String(), Serdes.String(), "wikipedia-message");

    wikipediaRaw.map(WikipediaMessageParser::parseMessage).to(Serdes.String(), wikipediaChangeSerializer, "wikipedia-parsed");
    final KafkaStreams streams = new KafkaStreams(builder, props);
    streams.start();

    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));


  }
}

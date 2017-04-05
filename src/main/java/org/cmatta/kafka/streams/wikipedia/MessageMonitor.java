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
import org.cmatta.kafka.connect.irc.Message;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by chris on 10/15/16.
 */
public class MessageMonitor {
  public static void main(String[] args) throws Exception {
    Properties streamProps = new Properties();
    Properties appProps = new Properties();

    streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wikipedia-monitor");

    if(System.getenv("CHANGESMONITOR_BOOTSTRAP_SERVERS") != null) {
      streamProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("CHANGESMONITOR_BOOTSTRAP_SERVERS"));
    } else {
      streamProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
    }

    if(System.getenv("CHANGESMONITOR_ZOOKEEPER_CONFIG") != null) {
      streamProps.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, System.getenv("CHANGESMONITOR_ZOOKEEPER_CONFIG"));
    } else {
      streamProps.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "zookeeper:2181");
    }

    streamProps.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://schemaregistry:8081");
    streamProps.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamProps.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
    streamProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    streamProps.put("consumer.interceptor.classes", "io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor");
    streamProps.put("producer.interceptor.classes", "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor");

        // Override defaults with explicit input (if present)
    String[] pNames = {StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG};
    String pValue;
    if (args.length > 0) {
        System.out.println("Reading property overrides from "+args[0]);

            // Known properties that we can override
        InputStream propInputStream = null;

        try {
            propInputStream = new FileInputStream(args[0]);
            appProps.load(propInputStream);

            for (String prop : pNames) {
                pValue=appProps.getProperty(prop);
                if (pValue != null) {
                    System.out.println("  Overriding "+prop+" with "+pValue);
                    streamProps.put(prop, pValue);
                }
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            if (propInputStream != null) {
                propInputStream.close();
            }
        }
    }

        // Lastly, command-line overrides (highest precedence)
    System.out.println("Checking for command-line overrides");
    for (String prop : pNames) {
        pValue=System.getProperty(prop);
        if (pValue != null) {
            System.out.println("  Overriding "+prop+" with "+pValue);
            streamProps.put(prop, pValue);
        }
    }
    KStreamBuilder builder = new KStreamBuilder();

    KStream<String, Message> wikipediaRaw = builder.stream("wikipedia.raw");

    wikipediaRaw.map(WikipediaMessageParser::parseMessage)
        .filter((k, v) -> k != null && v != null).to("wikipedia.parsed");

//    TODO Add KTable example code
    final KafkaStreams streams = new KafkaStreams(builder, streamProps);
    streams.start();

    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));


  }
}

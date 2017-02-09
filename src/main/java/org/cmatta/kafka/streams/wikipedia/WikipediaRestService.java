package org.cmatta.kafka.streams.wikipedia;

import org.apache.kafka.common.serialization.StringSerializer;
import io.confluent.examples.streams.interactivequeries.KeyValueBean;
import io.confluent.examples.streams.interactivequeries.MetadataService;
import io.confluent.examples.streams.interactivequeries.HostStoreInfo;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Created by chris on 11/25/16.
 */
@Path("state")
public class WikipediaRestService {
    private final KafkaStreams streams;
    private final MetadataService metadataService;
    private Server jettyServer;

    WikipediaRestService(final KafkaStreams streams) {
        this.streams = streams;
        this.metadataService = new MetadataService(streams);
    }

    @GET()
    @Path("/keyvalues/{storeName}/all")
    @Produces(MediaType.APPLICATION_JSON)
    public List<KeyValueBean> allForStore(@PathParam("storeName") final String storeName) {
      return rangeForKeyValueStore(storeName, ReadOnlyKeyValueStore::all);
    }

    /**
   * Return Hello information
     * using this to learn
   */
  @GET()
  @Path("/hello")
  @Produces(MediaType.APPLICATION_JSON)
  public Hello getRoot() {
    return new Hello();
  }

  /**
   * Get the metadata for all of the instances of this Kafka Streams application
   * @return List of {@link HostStoreInfo}
   */
  @GET()
  @Path("/instances")
  @Produces(MediaType.APPLICATION_JSON)
  public List<HostStoreInfo> streamsMetadata() {
    return metadataService.streamsMetadata();
  }

  /**
   * Get the metadata for all instances of this Kafka Streams application that currently
   * has the provided store.
   * @param store   The store to locate
   * @return  List of {@link HostStoreInfo}
   */
  @GET()
  @Path("/instances/{storeName}")
  @Produces(MediaType.APPLICATION_JSON)
  public List<HostStoreInfo> streamsMetadataForStore(@PathParam("storeName") String store) {
    return metadataService.streamsMetadataForStore(store);
  }

  /**
   * Find the metadata for the instance of this Kafka Streams Application that has the given
   * store and would have the given key if it exists.
   * @param store   Store to find
   * @param key     The key to find
   * @return {@link HostStoreInfo}
   */
  @GET()
  @Path("/instance/{storeName}/{key}")
  @Produces(MediaType.APPLICATION_JSON)
  public HostStoreInfo streamsMetadataForStoreAndKey(@PathParam("storeName") String store,
                                                     @PathParam("key") String key) {
    return metadataService.streamsMetadataForStoreAndKey(store, key, new StringSerializer());
  }

  private class Hello {
    private String message;
    public Hello() {
      this.message = "Hello World";
    };

    public String getMessage() { return this.message; }

    public void setMessage(String message) {
      this.message = message;
    }

    @Override
    public String toString() {
      return this.getMessage();
    }


  }

  private List<KeyValueBean> rangeForKeyValueStore(final String storeName,
                                                     final Function<ReadOnlyKeyValueStore<String, Long>,
                                                         KeyValueIterator<String, Long>> rangeFunction) {
//        Get the KeyValueStore
        final ReadOnlyKeyValueStore<String, Long> store = streams.store(storeName, QueryableStoreTypes.keyValueStore());
        if(store == null) {
            throw new NotFoundException();
        }

        final List<KeyValueBean> results = new ArrayList<>();

        final KeyValueIterator<String, Long> range = rangeFunction.apply(store);

        while(range.hasNext()) {
            final KeyValue<String, Long> next = range.next();
            results.add(new KeyValueBean(next.key, next.value));
        }

        return  results;
    }

//    Start embedded Jetty Server
    void start(final int port) throws Exception {
      ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
      context.setContextPath("/");

      jettyServer = new Server(port);
      jettyServer.setHandler(context);

      ResourceConfig rc = new ResourceConfig();
      rc.register(this);
      rc.register(JacksonFeature.class);

      ServletContainer sc = new ServletContainer(rc);
      ServletHolder holder = new ServletHolder(sc);
      context.addServlet(holder, "/*");

      jettyServer.start();
    }

    void stop() throws Exception {
      if (jettyServer != null) {
        jettyServer.stop();
      }
    }

}

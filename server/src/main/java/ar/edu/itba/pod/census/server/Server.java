package ar.edu.itba.pod.census.server;

import ar.edu.itba.pod.census.model.SerialDataFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;

import java.io.FileNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Server {
  private static final Logger LOGGER = LoggerFactory.getLogger(Server.class);

  private Server() {
  }

  public static void main(String[] args) throws FileNotFoundException {
    LOGGER.debug("Server starting...");
    createHazelcastServer();
    LOGGER.info("Server started");
  }

  private static void createHazelcastServer() throws FileNotFoundException {
    final Config serverConfig = new XmlConfigBuilder(
        System.getProperty("user.dir") + "/hazelcast.xml")
        .build();
    serverConfig.getSerializationConfig()
            .addDataSerializableFactory(SerialDataFactory.FACTORY_ID, new SerialDataFactory());

    Hazelcast.newHazelcastInstance(serverConfig);
  }
}

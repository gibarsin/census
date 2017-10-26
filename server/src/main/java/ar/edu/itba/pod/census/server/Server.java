package ar.edu.itba.pod.census.server;

import ar.edu.itba.pod.census.config.SharedConfiguration;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Server {

  private static Logger LOGGER = LoggerFactory.getLogger(Server.class);

  private Server() {
  }

  public static void main(String[] args) {
    LOGGER.debug("Server starting...");
    final HazelcastInstance hazelcastServer = createHazelcastServer();
    LOGGER.info("Server started");
  }

  private static HazelcastInstance createHazelcastServer() {
    final Config serverConfig = new Config();

    serverConfig.getGroupConfig()
        .setName(SharedConfiguration.GROUP_USERNAME)
        .setPassword(SharedConfiguration.GROUP_PASSWORD);

    return Hazelcast.newHazelcastInstance(serverConfig);
  }
}

package ar.edu.itba.pod.census.client;

import ar.edu.itba.pod.census.ProvinceCitizen;
import ar.edu.itba.pod.census.SharedConfiguration;
import ar.edu.itba.pod.census.client.CensusCSVRecords.Headers;
import ar.edu.itba.pod.census.client.args.ClientArgs;
import com.beust.jcommander.JCommander;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Client {

  private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);

  private static final ClientArgs CLIENT_ARGS = ClientArgs.getInstance();

  private static final HazelcastInstance HAZELCAST_CLIENT = createHazelcastClient();

  public static void main(final String[] args) {
    parseClientArguments(args);
    loadCSTFileAndPopulateHazelcastMap(CLIENT_ARGS.getInPath(),
        Client::populateHazelcastMapWithRecords);
  }

  private static void parseClientArguments(final String[] args) {
    JCommander.newBuilder()
        .addObject(CLIENT_ARGS)
        .defaultProvider(ClientArgs.SYSTEM_PROPERTIES_PROVIDER)
        .build()
        .parse(args);
  }

  private static void loadCSTFileAndPopulateHazelcastMap(final String path,
      final Consumer<Iterator<CSVRecord>> consumer) {
    try (final CensusCSVRecords csvRecords = new CensusCSVRecords(path)) {
      consumer.accept(csvRecords);
    } catch (final IOException exception) {
      System.err.println("There was an error while trying to read the input file");
      LOGGER.error("Could not open/read input file", exception);
      System.exit(1);
    }
  }

  // Query dependant
  private static void populateHazelcastMapWithRecords(final Iterator<CSVRecord> csvRecords) {
    final Map<String, List<ProvinceCitizen>> map =
        HAZELCAST_CLIENT.getMap(SharedConfiguration.getMapName());

    csvRecords.forEachRemaining(record -> {
      final ProvinceCitizen citizenToAdd = new ProvinceCitizen(
          Integer.parseInt(record.get(Headers.EMPLOYMENT_STATUS)),
          Integer.parseInt(record.get(Headers.HOME_ID)),
          record.get(Headers.DEPARTMENT_NAME));

      map.computeIfAbsent(record.get(Headers.PROVINCE_NAME), province -> new ArrayList<>())
          .add(citizenToAdd);
    });
  }

  private static HazelcastInstance createHazelcastClient() {
    return HazelcastClient.newHazelcastClient(createClientConfigWithCredentials());
  }

  private static ClientConfig createClientConfigWithCredentials() {
    final ClientConfig clientConfig = new ClientConfig();

    clientConfig.getGroupConfig()
        .setName(SharedConfiguration.getGroupUsername())
        .setPassword(SharedConfiguration.getGroupPassword());

    return clientConfig;
  }
}

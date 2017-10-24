package ar.edu.itba.pod.census.client;

import ar.edu.itba.pod.census.ProvinceHabitant;
import ar.edu.itba.pod.census.SharedConfiguration;
import ar.edu.itba.pod.census.client.args.ClientArgs;
import com.beust.jcommander.JCommander;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Client {

  private final static Logger logger = LoggerFactory.getLogger(Client.class);

  private final static ClientArgs clientArgs = ClientArgs.getInstance();
  private final static HazelcastInstance hazelcast = createHazelcastClient();

  public static void main(final String[] args) {
    parseClientArguments(args);
    loadCSVFileInHazelcastMap();
  }

  private static void parseClientArguments(final String[] args) {
    JCommander.newBuilder().addObject(clientArgs).build().parse(args);
  }

  private static void loadCSVFileInHazelcastMap() {
    final Iterator<CSVRecord> csvRecords = new CensusCSVRecords(clientArgs.getInPath());
    populateMapWithRecords(csvRecords);
  }

  private static void populateMapWithRecords(final Iterator<CSVRecord> csvRecords) {
    final Map<String, List<ProvinceHabitant>> map = hazelcast
        .getMap(SharedConfiguration.getMapName());

    csvRecords.forEachRemaining(record -> {
      final ProvinceHabitant habitantToAdd = new ProvinceHabitant(Integer.parseInt(record.get(0)),
          Integer.parseInt(record.get(1)), record.get(2));
      map.computeIfAbsent(record.get(3), province -> new ArrayList<>()).add(habitantToAdd);
    });
  }

  private static HazelcastInstance createHazelcastClient() {
    final ClientConfig clientConfig = createClientConfigWithCredentials();
    return HazelcastClient.newHazelcastClient(clientConfig);
  }

  private static ClientConfig createClientConfigWithCredentials() {
    final ClientConfig clientConfig = new ClientConfig();
    clientConfig.getGroupConfig().setName(SharedConfiguration.getGroupUsername())
        .setPassword(SharedConfiguration.getGroupPassword());
    return clientConfig;
  }
}

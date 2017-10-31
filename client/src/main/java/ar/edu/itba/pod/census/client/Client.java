package ar.edu.itba.pod.census.client;

import ar.edu.itba.pod.census.client.args.ClientArgs;
import ar.edu.itba.pod.census.client.query.CitizensPerHomeInRegionQuery;
import ar.edu.itba.pod.census.client.query.DepartmentCountQuery;
import ar.edu.itba.pod.census.client.query.DepartmentPopulationQuery;
import ar.edu.itba.pod.census.client.query.HomesInRegionQuery;
import ar.edu.itba.pod.census.client.query.RegionOccupationQuery;
import ar.edu.itba.pod.census.client.query.RegionPopulationQuery;
import ar.edu.itba.pod.census.client.query.SharedDepartmentCountQuery;
import ar.edu.itba.pod.census.config.SharedConfiguration;
import ar.edu.itba.pod.census.model.Region;
import com.beust.jcommander.JCommander;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import java.io.Closeable;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("Duplicates") // TODO: Remove
public final class Client {

  private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);

  private static final ClientArgs CLIENT_ARGS = ClientArgs.getInstance();

  private Client() {
  }

  public static void main(final String[] args) {
    parseClientArguments(args);

    final HazelcastInstance hazelcastClient = createHazelcastClient();

    final CensusCSVRecords csvRecords = loadInputFile();
    fillData(hazelcastClient, csvRecords);
    close(csvRecords);

    handleQuery(hazelcastClient);

    hazelcastClient.shutdown();
  }

  private static void parseClientArguments(final String[] args) {
    JCommander.newBuilder()
        .addObject(CLIENT_ARGS)
        .defaultProvider(ClientArgs.SYSTEM_PROPERTIES_PROVIDER)
        .build()
        .parse(args);
  }

  private static HazelcastInstance createHazelcastClient() {
    final ClientConfig clientConfig = new ClientConfig();

    clientConfig.getNetworkConfig().setAddresses(CLIENT_ARGS.getAddresses());

    clientConfig.getGroupConfig()
        .setName(SharedConfiguration.GROUP_USERNAME)
        .setPassword(SharedConfiguration.GROUP_PASSWORD);

    return HazelcastClient.newHazelcastClient(clientConfig);
  }

  private static CensusCSVRecords loadInputFile() {
    CensusCSVRecords csvRecords = null;
    try {
      csvRecords = CensusCSVRecords.open(CLIENT_ARGS.getInPath());
    } catch (final IOException exception) {
      System.err.println("There was an error while trying to open/read the input file");
      LOGGER.error("Could not open/read input file", exception);
      System.exit(1);
    }

    return csvRecords;
  }

  private static void fillData(final HazelcastInstance hazelcastClient,
      final CensusCSVRecords records) {
    switch (CLIENT_ARGS.getQuery()) {
      case 1:
        RegionPopulationQuery.fillData(hazelcastClient, records);
        break;

      case 2:
        DepartmentPopulationQuery.fillData(hazelcastClient, records);
        break;

      case 3:
        RegionOccupationQuery.fillData(hazelcastClient, records);
        break;
      case 4:
        HomesInRegionQuery.fillData(hazelcastClient, records);
        break;
      case 5:
        CitizensPerHomeInRegionQuery.fillData(hazelcastClient, records);
        break;
      case 6:
        DepartmentCountQuery.fillData(hazelcastClient, records);
        break;
      case 7:
        SharedDepartmentCountQuery.fillData(hazelcastClient, records);
        break;
    }
  }

  private static void handleQuery(final HazelcastInstance hazelcastClient) {
    switch (CLIENT_ARGS.getQuery()) {
      case 1:
        handleQuery1(hazelcastClient);
        break;

      case 2:
        handleQuery2(hazelcastClient);
        break;

      case 3:
        handleQuery3(hazelcastClient);
        break;

      case 4:
        handleQuery4(hazelcastClient);
        break;
      case 5:
        handleQuery5(hazelcastClient);
        break;
      case 6:
        handleQuery6(hazelcastClient);
        break;
      case 7:
        handleQuery7(hazelcastClient);
        break;
    }
  }

  private static void handleQuery5(final HazelcastInstance hazelcastClient) {
    LOGGER.debug("Submitting job...");
    final ICompletableFuture<List<Entry<Region, BigDecimal>>> futureResponse =
        CitizensPerHomeInRegionQuery.start(hazelcastClient);
    LOGGER.info("Job submitted");

    try {
      final List<Entry<Region, BigDecimal>> response = futureResponse.get();
      LOGGER.info("Job successful");

      for (final Entry<Region, BigDecimal> entry : response) {
        System.out.println(entry.getKey() + " -> " + entry.getValue());
      }
    } catch (final InterruptedException | ExecutionException exception) {
      LOGGER.error("Job failed", exception);
    }
  }

  private static void handleQuery1(final HazelcastInstance hazelcastClient) {
    LOGGER.debug("Submitting job...");
    final ICompletableFuture<List<Entry<String, Integer>>> futureResponse =
        RegionPopulationQuery.start(hazelcastClient);
    LOGGER.info("Job submitted");

    try {
      final List<Entry<String, Integer>> response = futureResponse.get();
      LOGGER.info("Job successful");

      for (final Entry<String, Integer> entry : response) {
        System.out.println(entry.getKey() + " -> " + entry.getValue());
      }
    } catch (final InterruptedException | ExecutionException exception) {
      LOGGER.error("Job failed", exception);
    }
  }

  private static void handleQuery2(final HazelcastInstance hazelcastClient) {
    LOGGER.debug("Submitting job...");
    final ICompletableFuture<List<Entry<String, Integer>>> futureResponse = DepartmentPopulationQuery
        .start(hazelcastClient, CLIENT_ARGS.getProvince(), CLIENT_ARGS.getN());
    LOGGER.info("Job submitted");

    try {
      final List<Entry<String, Integer>> response = futureResponse.get();
      LOGGER.info("Job successful");
      for (final Entry<String, Integer> entry : response) {
        System.out.println(entry.getKey() + " -> " + entry.getValue());
      }
    } catch (final InterruptedException | ExecutionException exception) {
      LOGGER.error("Job failed", exception);
    }
  }

  private static void handleQuery3(final HazelcastInstance hazelcastClient) {
    LOGGER.debug("Submitting job...");
    final ICompletableFuture<Map<String, BigDecimal>> futureResponse =
        RegionOccupationQuery.start(hazelcastClient);
    LOGGER.info("Job submitted");

    try {
      final Map<String, BigDecimal> response = futureResponse.get();
      LOGGER.info("Job successful");
      for (final Map.Entry<String, BigDecimal> entry : response.entrySet()) {
        System.out.println(entry.getKey() + " -> " + entry.getValue());
      }
    } catch (final InterruptedException | ExecutionException exception) {
      LOGGER.error("Job failed", exception);
    }
  }

  private static void handleQuery4(final HazelcastInstance hazelcastClient) {
    LOGGER.debug("Submitting job...");
    final ICompletableFuture<List<Entry<String, Integer>>> futureResponse =
        HomesInRegionQuery.start(hazelcastClient);
    LOGGER.info("Job submitted");

    try {
      final List<Entry<String, Integer>> response = futureResponse.get();
      LOGGER.info("Job successful");

      for (final Entry<String, Integer> entry : response) {
        System.out.println(entry.getKey() + " -> " + entry.getValue());
      }
    } catch (final InterruptedException | ExecutionException exception) {
      LOGGER.error("Job failed", exception);
    }
  }

  private static void handleQuery6(final HazelcastInstance hazelcastClient) {
    LOGGER.debug("Submitting job...");
    final ICompletableFuture<List<Entry<String, Integer>>> futureResponse =
        DepartmentCountQuery.start(hazelcastClient, CLIENT_ARGS.getN());
    LOGGER.info("Job submitted");

    try {
      final List<Entry<String, Integer>> response = futureResponse.get();
      LOGGER.info("Job successful");

      for (final Entry<String, Integer> entry : response) {
        System.out.println(entry.getKey() + " -> " + entry.getValue());
      }
    } catch (final InterruptedException | ExecutionException exception) {
      LOGGER.error("Job failed", exception);
    }
  }

  private static void handleQuery7(final HazelcastInstance hazelcastClient) {
    LOGGER.debug("Submitting job...");
    final ICompletableFuture<List<Entry<String, Integer>>> futureResponse =
        SharedDepartmentCountQuery.start(hazelcastClient, CLIENT_ARGS.getN());
    LOGGER.info("Job submitted");

    try {
      final List<Entry<String, Integer>> response = futureResponse.get();
      LOGGER.info("Job successful");

      for (final Entry<String, Integer> entry : response) {
        System.out.println(entry.getKey() + " -> " + entry.getValue());
      }
    } catch (final InterruptedException | ExecutionException exception) {
      LOGGER.error("Job failed", exception);
    }
  }

  private static void close(final Closeable closeable) {
    try {
      closeable.close();
    } catch (final IOException exception) {
      LOGGER.error("There was an error while closing the file", exception);
    }
  }
}

package ar.edu.itba.pod.census.client;

import ar.edu.itba.pod.census.client.args.ClientArgs;
import ar.edu.itba.pod.census.client.query.*;
import ar.edu.itba.pod.census.config.SharedConfiguration;
import ar.edu.itba.pod.census.model.Region;
import com.beust.jcommander.JCommander;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

public final class Client {
  private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);
  private static final ClientArgs CLIENT_ARGS = ClientArgs.getInstance();

  // IMPORTANT: Ordinals are in use. Update this with caution
  private enum Query {
    REGION_POPULATION, DEPARTMENT_POPULATION, REGION_OCCUPATION,
    HOMES_IN_REGION, CITIZENS_PER_HOME_IN_REGION,
    DEPARTMENT_COUNT, SHARED_DEPARTMENT_COUNT
  }

  public static void main(final String[] args) {
    final HazelcastInstance hazelcastInstance = createHazelcastClient();
    parseClientArguments(args);
    final IQuery query = buildQuery(hazelcastInstance, CLIENT_ARGS);
    query.run();
    hazelcastInstance.shutdown();
  }

  private static IQuery buildQuery(final HazelcastInstance hazelcastInstance, final ClientArgs clientArgs) {
    // Queries index is offset by 1 to the left (i.e., query 1 is index 0, query 2 index 1 and so on...)
    final AbstractQuery.Builder builder = getBuilderForQuery(Query.values()[clientArgs.getQuery() - 1]);
    return builder.setHazelcastInstance(hazelcastInstance).setClientArgs(clientArgs).build();
  }

  private static AbstractQuery.Builder getBuilderForQuery(final Query query) {
    switch (query) {
      case REGION_POPULATION:
        return new RegionPopulationQuery.Builder();
      break;
      case DEPARTMENT_POPULATION:
        return new DepartmentPopulationQuery.Builder();
      break;
      case REGION_OCCUPATION:
        return new RegionOccupationQuery.Builder();
      break;
      case HOMES_IN_REGION:
        return new HomesInRegionQuery.Builder();
      break;
      case CITIZENS_PER_HOME_IN_REGION:
        return new CitizensPerHomeInRegionQuery.Builder();
      break;
      case DEPARTMENT_COUNT:
        return new DepartmentCountQuery.Builder();
      break;
      case SHARED_DEPARTMENT_COUNT:
        return new SharedDepartmentCountQuery.Builder();
      break;
      default:
        throw new IllegalStateException("No valid query selected");
    }
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
}

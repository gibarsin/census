package ar.edu.itba.pod.census.client;

import ar.edu.itba.pod.census.client.args.ClientArgs;
import ar.edu.itba.pod.census.client.exception.ArgumentsErrorException;
import ar.edu.itba.pod.census.client.exception.InputFileErrorException;
import ar.edu.itba.pod.census.client.exception.OutputFileErrorException;
import ar.edu.itba.pod.census.client.exception.QueryFailedException;
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
  private static final Logger LOGGER = LoggerFactory.getLogger(Client.class); // TODO: remove
  private static final ClientArgs CLIENT_ARGS = ClientArgs.getInstance();

  private enum Query {
    REGION_POPULATION, DEPARTMENT_POPULATION, REGION_OCCUPATION,
    HOMES_IN_REGION, CITIZENS_PER_HOME_IN_REGION,
    DEPARTMENT_COUNT, SHARED_DEPARTMENT_COUNT
  }

  /**
   * Queries index is offset by 1 to the left (i.e., query 1 is index 0, query 2 index 1 and so on...)
   */
  private static final Query[] QUERIES = new Query[Query.values().length];
  static {
    QUERIES[0] = Query.REGION_POPULATION;
    QUERIES[1] = Query.DEPARTMENT_POPULATION;
    QUERIES[2] = Query.REGION_OCCUPATION;
    QUERIES[3] = Query.HOMES_IN_REGION;
    QUERIES[4] = Query.CITIZENS_PER_HOME_IN_REGION;
    QUERIES[5] = Query.DEPARTMENT_COUNT;
    QUERIES[6] = Query.SHARED_DEPARTMENT_COUNT;
  }

  private enum ExitStatus {
    OK(0), ARGS_ERROR(1), INPUT_FILE_ERROR(2), QUERY_FAILED(3), OUT_FILE_ERROR(4);

    private final int status;
    ExitStatus(final int status) {
      this.status = status;
    }

    public int getStatus() {
      return status;
    }
  }

  public static void main(final String[] args) {
    ExitStatus exitStatus = ExitStatus.OK;
    // TODO: check if exceptions here can be handled if no hazelcast client can be grabbed/consider other hazelcast errors too
    final HazelcastInstance hazelcastInstance = createHazelcastClient();

    try {
      parseClientArguments(args);
      final IQuery query = buildQuery(hazelcastInstance, CLIENT_ARGS);
      query.run();
    } catch (final ArgumentsErrorException e) {
      System.err.println(e.getMessage());
      exitStatus = ExitStatus.ARGS_ERROR;
    } catch (final InputFileErrorException e) {
      System.err.println(e.getMessage());
      exitStatus = ExitStatus.INPUT_FILE_ERROR;
    } catch (final QueryFailedException e) {
      System.err.println(e.getMessage());
      exitStatus = ExitStatus.QUERY_FAILED;
    } catch (final OutputFileErrorException e) {
      System.err.println(e.getMessage());
      exitStatus = ExitStatus.OUT_FILE_ERROR;
    }

    hazelcastInstance.shutdown();

    // As we are writing a command line tool, we need this :)
    System.exit(exitStatus.getStatus());
  }

  private static IQuery buildQuery(final HazelcastInstance hazelcastInstance, final ClientArgs clientArgs)
          throws ArgumentsErrorException { // TODO: Add arguments validation on build
    final AbstractQuery.Builder builder = getBuilderForQuery(QUERIES[clientArgs.getQuery() - 1]);
    return builder.setHazelcastInstance(hazelcastInstance).setClientArgs(clientArgs).build();
  }

  private static AbstractQuery.Builder getBuilderForQuery(final Query query) {
    switch (query) {
      case REGION_POPULATION:
        return new RegionPopulationQuery.Builder();
      case DEPARTMENT_POPULATION:
        return new DepartmentPopulationQuery.Builder();
      case REGION_OCCUPATION:
        return new RegionOccupationQuery.Builder();
      case HOMES_IN_REGION:
        return new HomesInRegionQuery.Builder();
      case CITIZENS_PER_HOME_IN_REGION:
        return new CitizensPerHomeInRegionQuery.Builder();
      case DEPARTMENT_COUNT:
        return new DepartmentCountQuery.Builder();
      case SHARED_DEPARTMENT_COUNT:
        return new SharedDepartmentCountQuery.Builder();
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

  private static void handleQuery2(final HazelcastInstance hazelcastClient) throws ArgumentsErrorException {
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

  private static void handleQuery6(final HazelcastInstance hazelcastClient) throws ArgumentsErrorException {
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

  private static void handleQuery7(final HazelcastInstance hazelcastClient) throws ArgumentsErrorException {
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

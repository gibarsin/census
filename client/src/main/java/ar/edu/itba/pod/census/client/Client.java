package ar.edu.itba.pod.census.client;

import ar.edu.itba.pod.census.client.args.ClientArgs;
import ar.edu.itba.pod.census.client.args.CustomArgsValidator;
import ar.edu.itba.pod.census.client.exception.InputFileErrorException;
import ar.edu.itba.pod.census.client.exception.OutputFileErrorException;
import ar.edu.itba.pod.census.client.exception.QueryFailedException;
import ar.edu.itba.pod.census.client.query.*;
import ar.edu.itba.pod.census.config.SharedConfiguration;
import com.beust.jcommander.JCommander;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;

public final class Client {
  private enum Query {
    REGION_POPULATION, DEPARTMENT_POPULATION, REGION_OCCUPATION,
    HOME_COUNT_PER_REGION, CITIZENS_PER_HOME_BY_REGION,
    DEPARTMENT_COUNT, SHARED_DEPARTMENT_COUNT
  }

  /**
   * Queries index is offset by 1 to the left (i.e., query 1 is index 0, query 2 index 1 and so on...).
   * This is done in this way so as not to depend in the ordinal of each query
   */
  private static final Query[] QUERIES = new Query[Client.Query.values().length];

  static {
    QUERIES[0] = Client.Query.REGION_POPULATION;
    QUERIES[1] = Client.Query.DEPARTMENT_POPULATION;
    QUERIES[2] = Client.Query.REGION_OCCUPATION;
    QUERIES[3] = Client.Query.HOME_COUNT_PER_REGION;
    QUERIES[4] = Client.Query.CITIZENS_PER_HOME_BY_REGION;
    QUERIES[5] = Client.Query.DEPARTMENT_COUNT;
    QUERIES[6] = Client.Query.SHARED_DEPARTMENT_COUNT;
  }

  private enum ExitStatus {
    // 1 is used by hazelcast connections errors & argument validations
    OK(0), INPUT_FILE_ERROR(2), QUERY_FAILED(3), OUT_FILE_ERROR(4);

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
    final ClientArgs clientArgs = parseClientArguments(args);
    CustomArgsValidator.validate(clientArgs);
    final HazelcastInstance hazelcastInstance = createHazelcastClient(clientArgs);

    try {
      final ar.edu.itba.pod.census.client.query.Query query = buildQuery(hazelcastInstance, clientArgs);
      query.run();
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

  private static ar.edu.itba.pod.census.client.query.Query buildQuery(final HazelcastInstance hazelcastInstance,
                                                                      final ClientArgs clientArgs) {
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
      case HOME_COUNT_PER_REGION:
        return new HomeCountPerRegionQuery.Builder();
      case CITIZENS_PER_HOME_BY_REGION:
        return new CitizensPerHomeByRegionQuery.Builder();
      case DEPARTMENT_COUNT:
        return new PopularDepartmentNamesQuery.Builder();
      case SHARED_DEPARTMENT_COUNT:
        return new PopularDepartmentSharedCountQuery.Builder();
      default:
        throw new IllegalStateException("No valid query selected");
    }
  }

  private static ClientArgs parseClientArguments(final String[] args) {
    final ClientArgs clientArgs = ClientArgs.getInstance();
    JCommander.newBuilder()
        .addObject(clientArgs)
        .defaultProvider(ClientArgs.SYSTEM_PROPERTIES_PROVIDER)
        .build()
        .parse(args);
    return clientArgs;
  }

  private static HazelcastInstance createHazelcastClient(final ClientArgs clientArgs) {
    final ClientConfig clientConfig = new ClientConfig();

    clientConfig.getNetworkConfig().setAddresses(clientArgs.getAddresses());
    
    clientConfig.getGroupConfig()
        .setName(SharedConfiguration.GROUP_USERNAME)
        .setPassword(SharedConfiguration.GROUP_PASSWORD);

    return HazelcastClient.newHazelcastClient(clientConfig);
  }
}

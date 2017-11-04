package ar.edu.itba.pod.census.client;

import ar.edu.itba.pod.census.client.args.ClientArgs;
import ar.edu.itba.pod.census.client.exception.ArgumentsErrorException;
import ar.edu.itba.pod.census.client.exception.InputFileErrorException;
import ar.edu.itba.pod.census.client.exception.OutputFileErrorException;
import ar.edu.itba.pod.census.client.exception.QueryFailedException;
import ar.edu.itba.pod.census.client.query.*;
import ar.edu.itba.pod.census.config.SharedConfiguration;
import com.beust.jcommander.JCommander;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

public final class Client {
  private static final Logger LOGGER = LoggerFactory.getLogger(Client.class); // TODO: remove
  private static final ClientArgs CLIENT_ARGS = ClientArgs.getInstance();

  private enum Query {
    REGION_POPULATION, DEPARTMENT_POPULATION, REGION_OCCUPATION,
    HOME_COUNT_PER_REGION, CITIZENS_PER_HOME_BY_REGION,
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
    QUERIES[3] = Query.HOME_COUNT_PER_REGION;
    QUERIES[4] = Query.CITIZENS_PER_HOME_BY_REGION;
    QUERIES[5] = Query.DEPARTMENT_COUNT;
    QUERIES[6] = Query.SHARED_DEPARTMENT_COUNT;
  }

  private enum ExitStatus {
    // 1 is used by hazelcast connections errors
    OK(0), ARGS_ERROR(2), INPUT_FILE_ERROR(3), QUERY_FAILED(4), OUT_FILE_ERROR(5);

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
}

package ar.edu.itba.pod.census.client;

import ar.edu.itba.pod.census.client.args.ClientArgs;
import ar.edu.itba.pod.census.combiner.RegionPopulationCombinerFactory;
import ar.edu.itba.pod.census.config.SharedConfiguration;
import ar.edu.itba.pod.census.mapper.RegionPopulationMapper;
import ar.edu.itba.pod.census.model.Citizen;
import ar.edu.itba.pod.census.reducer.RegionPopulationReducerFactory;
import com.beust.jcommander.JCommander;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.CombinerFactory;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.mapreduce.ReducerFactory;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Client {

  private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);

  private static final ClientArgs CLIENT_ARGS = ClientArgs.getInstance();

  private static final HazelcastInstance HAZELCAST_CLIENT = createHazelcastClient(); // TODO: Move to main()

  private Client() {
  }

  public static void main(final String[] args) {
    parseClientArguments(args);
    loadInputFileAndPopulateHazelcastMap(CLIENT_ARGS.getInPath(),
        Client::populateHazelcastMapWithRecords);
    startHazelcastJob();
    // TODO: Shutdown hazelcast
  }

  private static void parseClientArguments(final String[] args) {
    JCommander.newBuilder()
        .addObject(CLIENT_ARGS)
        .defaultProvider(ClientArgs.SYSTEM_PROPERTIES_PROVIDER)
        .build()
        .parse(args);
  }

  private static void loadInputFileAndPopulateHazelcastMap(final String path,
      final Consumer<Iterator<Citizen>> consumer) {
    try (final CensusCSVRecords csvRecords = new CensusCSVRecords(path)) {
      consumer.accept(csvRecords);
    } catch (final IOException exception) {
      System.err.println("There was an error while trying to read the input file");
      LOGGER.error("Could not open/read input file", exception);
      System.exit(1);
    }
  }

  // Query dependant
  private static void populateHazelcastMapWithRecords(final Iterator<Citizen> citizenRecords) {
    final Map<Long, Citizen> map = HAZELCAST_CLIENT.getMap(SharedConfiguration.MAP_NAME);

    long id = 0;
    while (citizenRecords.hasNext()) {
      final Citizen citizen = citizenRecords.next();
      map.put(id++, citizen);
    }
  }

  // Query dependant
  private static void startHazelcastJob() {
    LOGGER.debug("Submitting job...");
    final JobTracker jobTracker = HAZELCAST_CLIENT.getJobTracker(SharedConfiguration.TRACKER_NAME);
    final IMap<Long, Citizen> map = HAZELCAST_CLIENT.getMap(SharedConfiguration.MAP_NAME);
    final KeyValueSource<Long, Citizen> source = KeyValueSource.fromMap(map);
    final Job<Long, Citizen> job = jobTracker.newJob(source);

    final Mapper<Long, Citizen, String, Long> mapper = new RegionPopulationMapper();
    final CombinerFactory<String, Long, Long> combinerFactory = new RegionPopulationCombinerFactory();
    final ReducerFactory<String, Long, Long> reducerFactory = new RegionPopulationReducerFactory();
    final ICompletableFuture<Map<String, Long>> futureResponse = job
        .mapper(mapper)
        .combiner(combinerFactory)
        .reducer(reducerFactory)
        .submit();
    LOGGER.info("Job submitted");

    futureResponse.andThen(new ExecutionCallback<>() {
      @Override
      public void onResponse(final Map<String, Long> response) {
        LOGGER.info("Job successful");
        for (final Map.Entry<String, Long> entry : response.entrySet()) {
          System.out.println(entry.getKey() + " -> " + entry.getValue());
        }
      }

      @Override
      public void onFailure(final Throwable t) {
        LOGGER.error("Job failed", t);
      }
    });
  }

  private static HazelcastInstance createHazelcastClient() {
    return HazelcastClient.newHazelcastClient(createClientConfigWithCredentials());
  }

  private static ClientConfig createClientConfigWithCredentials() {
    final ClientConfig clientConfig = new ClientConfig();

    clientConfig.getGroupConfig()
        .setName(SharedConfiguration.GROUP_USERNAME)
        .setPassword(SharedConfiguration.GROUP_PASSWORD);

    return clientConfig;
  }
}

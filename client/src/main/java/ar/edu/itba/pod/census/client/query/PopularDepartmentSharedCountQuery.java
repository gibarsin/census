package ar.edu.itba.pod.census.client.query;

import ar.edu.itba.pod.census.client.CensusCSVRecords.Headers;
import ar.edu.itba.pod.census.client.args.ClientArgs;
import ar.edu.itba.pod.census.collator.PopularDepartmentSharedCountCollator;
import ar.edu.itba.pod.census.combiner.PopularDepartmentCombinerFactory;
import ar.edu.itba.pod.census.config.SharedConfiguration;
import ar.edu.itba.pod.census.mapper.PopularDepartmentMapper;
import ar.edu.itba.pod.census.model.Container;
import ar.edu.itba.pod.census.model.ProvincePair;
import ar.edu.itba.pod.census.reducer.PopularDepartmentSharedReducerFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.*;

import java.io.PrintStream;
import java.util.*;
import java.util.concurrent.ExecutionException;

@SuppressWarnings("deprecation")
// Intentionally as we are using deprecated Hazelcast features
public final class PopularDepartmentSharedCountQuery extends AbstractQuery {
  private final int requiredN;

  private int key;
  private Map<Integer, Container> localInput;
  private IMap<Integer, Container> remoteInput;
  private ReducingSubmittableJob<Integer, String, Set<ProvincePair>> mapReducerJob;
  private Collator<Map.Entry<String, Set<ProvincePair>>, List<Map.Entry<String, Integer>>> collator;
  private List<Map.Entry<String, Integer>> jobResult;

  private PopularDepartmentSharedCountQuery(final HazelcastInstance hazelcastInstance, final ClientArgs clientArgs) {
    super(hazelcastInstance, clientArgs);
    requiredN = clientArgs.getN();
  }

  @Override
  protected void pickAClearClusterCollection(final HazelcastInstance hazelcastInstance) {
    localInput = new HashMap<>();
    key = 0;
    remoteInput = hazelcastInstance.getMap(SharedConfiguration.STRUCTURE_NAME);
    remoteInput.clear();
  }

  @Override
  protected void addRecordToClusterCollection(final String[] csvRecord) {
    localInput.put(key ++, new Container(
            -1,
            -1,
            csvRecord[Headers.DEPARTMENT_NAME.getColumn()].trim(),
            csvRecord[Headers.PROVINCE_NAME.getColumn()].trim()
    ));
  }

  @Override
  protected void submitAllRecordsToCluster() {
    remoteInput.putAll(localInput);
  }

  @Override
  protected void prepareJobResources(final JobTracker jobTracker) {
    // Create the custom job
    final KeyValueSource<Integer, Container> source = KeyValueSource.fromMap(remoteInput);
    final Job<Integer, Container> job = jobTracker.newJob(source);

    // Prepare the map reduce job to be submitted
    mapReducerJob = job.mapper(new PopularDepartmentMapper())
            .combiner(new PopularDepartmentCombinerFactory())
            .reducer(new PopularDepartmentSharedReducerFactory());

    // Prepare the collator to post-process the job's result
    // Compiler complains if we do not set this explicitly
    //noinspection Convert2Diamond
    collator = new PopularDepartmentSharedCountCollator(requiredN, Collections.reverseOrder(Map.Entry.comparingByValue()));
  }

  @Override
  protected void submitJob() throws ExecutionException, InterruptedException {
    jobResult = mapReducerJob.submit(collator).get();
  }

  @Override
  protected void processJobResult(final PrintStream output) {
    jobResult.forEach(entry -> output.println(entry.getKey() + "," + entry.getValue()));
  }

  public static class Builder extends AbstractQuery.Builder {
    @Override
    protected AbstractQuery build(final HazelcastInstance hazelcastInstance, final ClientArgs clientArgs) {
      return new PopularDepartmentSharedCountQuery(hazelcastInstance, clientArgs);
    }
  }
}

package ar.edu.itba.pod.census.client.query;

import ar.edu.itba.pod.census.client.CensusCSVRecords.Headers;
import ar.edu.itba.pod.census.client.args.ClientArgs;
import ar.edu.itba.pod.census.collator.MinIntegerValueSortCollator;
import ar.edu.itba.pod.census.combiner.PopularDepartmentCombinerFactory;
import ar.edu.itba.pod.census.config.SharedConfiguration;
import ar.edu.itba.pod.census.mapper.PopularDepartmentMapper;
import ar.edu.itba.pod.census.reducer.PopularDepartmentNamesReducerFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MultiMap;
import com.hazelcast.mapreduce.Collator;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.ReducingSubmittableJob;
import java.io.PrintStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public final class PopularDepartmentNamesQuery extends AbstractQuery {
  private final int requiredN;

  private MultiMap<String, String> remoteInput;
  private ReducingSubmittableJob<String, String, Integer> mapReducerJob;
  private Collator<Map.Entry<String, Integer>, List<Map.Entry<String, Integer>>> collator;
  private List<Map.Entry<String, Integer>> jobResult;

  private PopularDepartmentNamesQuery(final HazelcastInstance hazelcastInstance, final ClientArgs clientArgs) {
    super(hazelcastInstance, clientArgs);
    this.requiredN = clientArgs.getN();
  }

  @Override
  protected void pickAClearClusterCollection(final HazelcastInstance hazelcastInstance) {
    remoteInput = hazelcastInstance.getMultiMap(SharedConfiguration.STRUCTURE_NAME);
    remoteInput.clear();
  }

  @Override
  protected void addRecordToClusterCollection(final String[] csvRecord) {
    remoteInput.put(csvRecord[Headers.DEPARTMENT_NAME.getColumn()].trim(),
              csvRecord[Headers.PROVINCE_NAME.getColumn()].trim());
  }

  @Override
  protected void submitAllRecordsToCluster() {
    // Do nothing
  }

  @Override
  protected void prepareJobResources(final JobTracker jobTracker) {
    // Create the custom job
    final KeyValueSource<String, String> source = KeyValueSource.fromMultiMap(remoteInput);
    final Job<String, String> job = jobTracker.newJob(source);

    // Prepare the map reduce job to be submitted
    mapReducerJob = job.mapper(new PopularDepartmentMapper())
            .combiner(new PopularDepartmentCombinerFactory())
            .reducer(new PopularDepartmentNamesReducerFactory());

    // Prepare the collator to post-process the job's result
    // Compiler complains if we do not set this explicitly
    //noinspection Convert2Diamond
    collator = new MinIntegerValueSortCollator<>(requiredN, Collections.reverseOrder(Map.Entry.comparingByValue()));
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
      return new PopularDepartmentNamesQuery(hazelcastInstance, clientArgs);
    }
  }
}

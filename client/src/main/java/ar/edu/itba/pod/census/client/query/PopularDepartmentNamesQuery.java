package ar.edu.itba.pod.census.client.query;

import ar.edu.itba.pod.census.client.CSVHeaders;
import ar.edu.itba.pod.census.client.args.ClientArgs;
import ar.edu.itba.pod.census.collator.MinIntegerValueSortCollator;
import ar.edu.itba.pod.census.combiner.PopularDepartmentCombinerFactory;
import ar.edu.itba.pod.census.config.SharedConfiguration;
import ar.edu.itba.pod.census.mapper.PopularDepartmentMapper;
import ar.edu.itba.pod.census.model.Container;
import ar.edu.itba.pod.census.reducer.PopularDepartmentNamesReducerFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.*;

import java.io.PrintStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@SuppressWarnings("deprecation")
// Intentionally as we are using deprecated Hazelcast features
public final class PopularDepartmentNamesQuery extends AbstractQuery {
  private final int requiredN;

  private int key;
  private Map<Integer, Container> localInput;
  private IMap<Integer, Container> remoteInput;
  private ReducingSubmittableJob<Integer, String, Integer> mapReducerJob;
  private Collator<Map.Entry<String, Integer>, List<Map.Entry<String, Integer>>> collator;
  private List<Map.Entry<String, Integer>> jobResult;

  private PopularDepartmentNamesQuery(final HazelcastInstance hazelcastInstance, final ClientArgs clientArgs) {
    super(hazelcastInstance, clientArgs);
    this.requiredN = clientArgs.getN();
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
              csvRecord[CSVHeaders.DEPARTMENT_NAME.getColumn()].trim(),
              csvRecord[CSVHeaders.PROVINCE_NAME.getColumn()].trim()
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

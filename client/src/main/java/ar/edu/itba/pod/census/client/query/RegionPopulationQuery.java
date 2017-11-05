package ar.edu.itba.pod.census.client.query;

import ar.edu.itba.pod.census.client.CensusCSVRecords.Headers;
import ar.edu.itba.pod.census.client.args.ClientArgs;
import ar.edu.itba.pod.census.collator.SortCollator;
import ar.edu.itba.pod.census.combiner.NoKeyAdderCombinerFactory;
import ar.edu.itba.pod.census.config.SharedConfiguration;
import ar.edu.itba.pod.census.mapper.RegionPopulationMapper;
import ar.edu.itba.pod.census.reducer.NoKeyAdderReducerFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.*;

import java.io.PrintStream;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

@SuppressWarnings("deprecation")
// Intentionally as we are using deprecated Hazelcast features
public final class RegionPopulationQuery extends AbstractQuery {

  private List<String> localInput = new LinkedList<>();
  private IList<String> remoteInput;
  private ReducingSubmittableJob<String, String, Integer> mapReducerJob;
  private Collator<Entry<String, Integer>, List<Entry<String, Integer>>> collator;
  private List<Entry<String, Integer>> jobResult;

  private RegionPopulationQuery(final HazelcastInstance hazelcastInstance, final ClientArgs clientArgs) {
    super(hazelcastInstance, clientArgs);
  }

  @Override
  protected void pickAClearClusterCollection(final HazelcastInstance hazelcastInstance) {
    remoteInput = hazelcastInstance.getList(SharedConfiguration.STRUCTURE_NAME);
    remoteInput.clear();
  }

  @Override
  protected void addRecordToClusterCollection(final String[] csvRecord) {
    localInput.add(csvRecord[Headers.PROVINCE_NAME.getColumn()]);
  }

  @Override
  protected void submitAllRecordsToCluster() {
    remoteInput.addAll(localInput);
  }

  @Override
  protected void prepareJobResources(final JobTracker jobTracker) {
    // Create the custom job
    final KeyValueSource<String, String> source = KeyValueSource.fromList(remoteInput);
    final Job<String, String> job = jobTracker.newJob(source);

    // Prepare the map reduce job to be submitted
    mapReducerJob = job.mapper(new RegionPopulationMapper())
            .combiner(new NoKeyAdderCombinerFactory())
            .reducer(new NoKeyAdderReducerFactory());

    // Prepare the collator to post-process the job's result
    // Compiler complains if we do not set this explicitly
    //noinspection Convert2Diamond
    collator = new SortCollator<String, Integer>(Collections.reverseOrder(Entry.comparingByValue()));
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
      return new RegionPopulationQuery(hazelcastInstance, clientArgs);
    }
  }
}

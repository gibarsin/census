package ar.edu.itba.pod.census.client.query;

import ar.edu.itba.pod.census.client.CensusCSVRecords.Headers;
import ar.edu.itba.pod.census.client.args.ClientArgs;
import ar.edu.itba.pod.census.collator.SortCollator;
import ar.edu.itba.pod.census.combiner.NoKeyAdderCombinerFactory;
import ar.edu.itba.pod.census.config.SharedConfiguration;
import ar.edu.itba.pod.census.mapper.HomeCountPerRegionMapper;
import ar.edu.itba.pod.census.reducer.NoKeyAdderReducerFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.*;

import java.io.PrintStream;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

@SuppressWarnings("deprecation")
// Intentionally as we are using deprecated Hazelcast features
public final class HomeCountPerRegionQuery extends AbstractQuery {
  private IMap<Integer, String> input;
  private ReducingSubmittableJob<Integer, String, Integer> mapReducerJob;
  private Collator<Entry<String, Integer>, List<Entry<String, Integer>>> collator;
  private List<Entry<String, Integer>> jobResult;

  private HomeCountPerRegionQuery(final HazelcastInstance hazelcastInstance, final ClientArgs clientArgs) {
    super(hazelcastInstance, clientArgs);
  }

  @Override
  protected void pickAClearClusterCollection(final HazelcastInstance hazelcastInstance) {
    input = hazelcastInstance.getMap(SharedConfiguration.STRUCTURE_NAME);
    input.clear();
  }

  @Override
  protected void addRecordToClusterCollection(final String[] csvRecord) {
    input.put(
            Integer.parseInt(csvRecord[Headers.HOME_ID.getColumn()].trim()),
            csvRecord[Headers.PROVINCE_NAME.getColumn()].trim()
    );
  }

  @Override
  protected void prepareJobResources(final JobTracker jobTracker) {
    // Create the custom job
    final KeyValueSource<Integer, String> source = KeyValueSource.fromMap(input);
    final Job<Integer, String> job = jobTracker.newJob(source);

    // Prepare the map reduce job to be submitted
    mapReducerJob = job
            .mapper(new HomeCountPerRegionMapper())
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
      return new HomeCountPerRegionQuery(hazelcastInstance, clientArgs);
    }
  }
}

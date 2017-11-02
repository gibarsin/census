package ar.edu.itba.pod.census.client.query;

import ar.edu.itba.pod.census.client.CensusCSVRecords;
import ar.edu.itba.pod.census.client.CensusCSVRecords.Headers;
import ar.edu.itba.pod.census.client.args.ClientArgs;
import ar.edu.itba.pod.census.collator.SortCollator;
import ar.edu.itba.pod.census.combiner.DepartmentPopulationCombinerFactory;
import ar.edu.itba.pod.census.config.SharedConfiguration;
import ar.edu.itba.pod.census.mapper.HomesInRegionMapper;
import ar.edu.itba.pod.census.model.Home;
import ar.edu.itba.pod.census.reducer.HomesInRegionReducerFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.ISet;
import com.hazelcast.mapreduce.Collator;
import com.hazelcast.mapreduce.CombinerFactory;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.mapreduce.ReducerFactory;

import java.io.PrintStream;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.commons.csv.CSVRecord;

public final class HomesInRegionQuery extends AbstractQuery {
  private HomesInRegionQuery(final HazelcastInstance hazelcastInstance, final ClientArgs clientArgs) {
    super(hazelcastInstance, clientArgs);
  }

  public static void fillData(final HazelcastInstance hazelcastInstance,
                              final CensusCSVRecords citizenRecords) {
    final Set<Home> set = hazelcastInstance.getSet(SharedConfiguration.STRUCTURE_NAME);

    while (citizenRecords.hasNext()) {
      final CSVRecord record = citizenRecords.next();

      set.add(new Home(
          Integer.parseInt(record.get(Headers.HOME_ID).trim()),
          record.get(Headers.DEPARTMENT_NAME).trim(),
          record.get(Headers.PROVINCE_NAME).trim()));
    }
  }

  public static ICompletableFuture<List<Entry<String, Integer>>> start(
      final HazelcastInstance hazelcastInstance) {
    final JobTracker jobTracker = hazelcastInstance.getJobTracker(SharedConfiguration.TRACKER_NAME);
    final ISet<Home> input = hazelcastInstance.getSet(SharedConfiguration.STRUCTURE_NAME);
    final KeyValueSource<String, Home> source = KeyValueSource.fromSet(input);
    final Job<String, Home> job = jobTracker.newJob(source);

    final Mapper<String, Home, String, Integer> mapper = new HomesInRegionMapper();
    final CombinerFactory<String, Integer, Integer> combinerFactory = new DepartmentPopulationCombinerFactory();
    final ReducerFactory<String, Integer, Integer> reducerFactory = new HomesInRegionReducerFactory();
    final Collator<Entry<String, Integer>, List<Entry<String, Integer>>> collator =
        new SortCollator<String, Integer>(Collections.reverseOrder(Entry.comparingByValue()));

    return job
        .mapper(mapper)
        .combiner(combinerFactory)
        .reducer(reducerFactory)
        .submit(collator);
  }

  @Override
  protected void pickAClearClusterCollection(final HazelcastInstance hazelcastInstance) {
    // TODO
  }

  @Override
  protected void addRecordToClusterCollection(final String[] csvRecord) {
    // TODO
  }

  @Override
  protected void prepareJobResources(final JobTracker jobTracker) {
    // TODO
  }

  @Override
  protected void submitJob() throws ExecutionException, InterruptedException {
    // TODO
  }

  @Override
  protected void processJobResult(final PrintStream output) {
    // TODO
  }

  public static class Builder extends AbstractQuery.Builder {
    @Override
    protected AbstractQuery build(final HazelcastInstance hazelcastInstance, final ClientArgs clientArgs) {
      return new HomesInRegionQuery(hazelcastInstance, clientArgs);
    }
  }
}

package ar.edu.itba.pod.census.client.query;

import ar.edu.itba.pod.census.client.CensusCSVRecords;
import ar.edu.itba.pod.census.client.CensusCSVRecords.Headers;
import ar.edu.itba.pod.census.client.args.ClientArgs;
import ar.edu.itba.pod.census.collator.MinIntegerValueSortCollator;
import ar.edu.itba.pod.census.combiner.DepartmentCountCombinerFactory;
import ar.edu.itba.pod.census.config.SharedConfiguration;
import ar.edu.itba.pod.census.mapper.DepartmentCountMapper;
import ar.edu.itba.pod.census.model.Department;
import ar.edu.itba.pod.census.reducer.DepartmentCountReducerFactory;
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

public final class DepartmentCountQuery extends AbstractQuery {
  private DepartmentCountQuery(final HazelcastInstance hazelcastInstance, final ClientArgs clientArgs) {
    super(hazelcastInstance, clientArgs);
  }

  public static void fillData(final HazelcastInstance hazelcastInstance,
      final CensusCSVRecords citizenRecords) {
    final Set<Department> set = hazelcastInstance.getSet(SharedConfiguration.STRUCTURE_NAME);

    while (citizenRecords.hasNext()) {
      final CSVRecord record = citizenRecords.next();

      set.add(new Department(
          record.get(Headers.DEPARTMENT_NAME).trim(),
          record.get(Headers.PROVINCE_NAME).trim()));
    }
  }

  public static ICompletableFuture<List<Entry<String, Integer>>> start(
      final HazelcastInstance hazelcastInstance, final int minValue) {
    final JobTracker jobTracker = hazelcastInstance.getJobTracker(SharedConfiguration.TRACKER_NAME);
    final ISet<Department> input = hazelcastInstance.getSet(SharedConfiguration.STRUCTURE_NAME);
    final KeyValueSource<String, Department> source = KeyValueSource.fromSet(input);
    final Job<String, Department> job = jobTracker.newJob(source);

    final Mapper<String, Department, String, Integer> mapper = new DepartmentCountMapper();
    final CombinerFactory<String, Integer, Integer> combinerFactory = new DepartmentCountCombinerFactory();
    final ReducerFactory<String, Integer, Integer> reducerFactory = new DepartmentCountReducerFactory();
    final Collator<Entry<String, Integer>, List<Entry<String, Integer>>> collator =
        new MinIntegerValueSortCollator<String>(minValue,
            Collections.reverseOrder(Entry.comparingByValue()));

    return job
        .mapper(mapper)
        .combiner(combinerFactory)
        .reducer(reducerFactory)
        .submit(collator);
  }

  @Override
  protected void getAClearClusterCollection(final HazelcastInstance hazelcastInstance) {
    // TODO
  }

  @Override
  protected void addRecordToClusterCollection(final String[] csvRecord) {
    // TODO
  }

  @Override
  protected void buildMapReduceJob(final JobTracker jobTracker) {
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
      return new DepartmentCountQuery(hazelcastInstance, clientArgs);
    }
  }
}

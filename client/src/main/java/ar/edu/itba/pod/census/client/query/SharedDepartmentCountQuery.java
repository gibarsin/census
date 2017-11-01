package ar.edu.itba.pod.census.client.query;

import ar.edu.itba.pod.census.client.CensusCSVRecords;
import ar.edu.itba.pod.census.client.CensusCSVRecords.Headers;
import ar.edu.itba.pod.census.client.args.ClientArgs;
import ar.edu.itba.pod.census.collator.MinIntegerValueSortCollator;
import ar.edu.itba.pod.census.config.SharedConfiguration;
import ar.edu.itba.pod.census.mapper.SharedDepartmentCountMapper;
import ar.edu.itba.pod.census.model.Province;
import ar.edu.itba.pod.census.reducer.SharedDepartmentCountReducerFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.MultiMap;
import com.hazelcast.mapreduce.Collator;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.mapreduce.ReducerFactory;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

import org.apache.commons.csv.CSVRecord;

public final class SharedDepartmentCountQuery extends AbstractQuery {
  private SharedDepartmentCountQuery(final HazelcastInstance hazelcastInstance, final ClientArgs clientArgs) {
    super(hazelcastInstance, clientArgs);
  }

  public static void fillData(final HazelcastInstance hazelcastInstance,
                              final CensusCSVRecords records) {
    final MultiMap<String, Province> map =
        hazelcastInstance.getMultiMap(SharedConfiguration.STRUCTURE_NAME);

    while (records.hasNext()) {
      final CSVRecord record = records.next();

      map.put(record.get(Headers.DEPARTMENT_NAME),
          Province.fromString(record.get(Headers.PROVINCE_NAME)));
    }
  }

  public static ICompletableFuture<List<Entry<String, Integer>>> start(
      final HazelcastInstance hazelcastInstance, final int minValue) {
    final JobTracker jobTracker = hazelcastInstance.getJobTracker(SharedConfiguration.TRACKER_NAME);
    final MultiMap<String, Province> multiMap =
        hazelcastInstance.getMultiMap(SharedConfiguration.STRUCTURE_NAME);
    final KeyValueSource<String, Province> source = KeyValueSource.fromMultiMap(multiMap);
    final Job<String, Province> job = jobTracker.newJob(source);

    final Mapper<String, Province, String, String> mapper = new SharedDepartmentCountMapper();
    final ReducerFactory<String, String, Integer> reducerFactory = new SharedDepartmentCountReducerFactory();
    final Collator<Entry<String, Integer>, List<Entry<String, Integer>>> collator =
        new MinIntegerValueSortCollator<String>(minValue,
            Collections.reverseOrder(Entry.comparingByValue()));

    return job
        .mapper(mapper)
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
  protected void processJobResult() {
    // TODO
  }

  public static class Builder extends AbstractQuery.Builder {
    @Override
    protected AbstractQuery build(final HazelcastInstance hazelcastInstance, final ClientArgs clientArgs) {
      return new SharedDepartmentCountQuery(hazelcastInstance, clientArgs);
    }
  }
}

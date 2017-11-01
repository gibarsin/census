package ar.edu.itba.pod.census.client.query;

import ar.edu.itba.pod.census.client.CensusCSVRecords;
import ar.edu.itba.pod.census.client.CensusCSVRecords.Headers;
import ar.edu.itba.pod.census.client.args.ClientArgs;
import ar.edu.itba.pod.census.collator.SortCollator;
import ar.edu.itba.pod.census.combiner.RegionPopulationCombinerFactory;
import ar.edu.itba.pod.census.config.SharedConfiguration;
import ar.edu.itba.pod.census.mapper.RegionPopulationMapper;
import ar.edu.itba.pod.census.model.Citizen;
import ar.edu.itba.pod.census.reducer.RegionPopulationReducerFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.Collator;
import com.hazelcast.mapreduce.CombinerFactory;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.mapreduce.ReducerFactory;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import org.apache.commons.csv.CSVRecord;

public final class RegionPopulationQuery extends AbstractQuery {
  private RegionPopulationQuery(final HazelcastInstance hazelcastInstance, final ClientArgs clientArgs) {
    super(hazelcastInstance, clientArgs);
  }

  public static void fillData(final HazelcastInstance hazelcastInstance,
                              final CensusCSVRecords records) {
    final List<Citizen> list = hazelcastInstance.getList(SharedConfiguration.STRUCTURE_NAME);

    while (records.hasNext()) {
      final CSVRecord record = records.next();

      list.add(new Citizen(
          Integer.parseInt(record.get(Headers.EMPLOYMENT_STATUS).trim()),
          Integer.parseInt(record.get(Headers.HOME_ID).trim()),
          record.get(Headers.DEPARTMENT_NAME),
          record.get(Headers.PROVINCE_NAME)));
    }
  }

  public static ICompletableFuture<List<Entry<String, Integer>>> start(
      final HazelcastInstance hazelcastInstance) {
    final JobTracker jobTracker = hazelcastInstance
        .getJobTracker(SharedConfiguration.TRACKER_NAME);
    final IList<Citizen> input = hazelcastInstance.getList(SharedConfiguration.STRUCTURE_NAME);
    final KeyValueSource<String, Citizen> source = KeyValueSource.fromList(input);
    final Job<String, Citizen> job = jobTracker.newJob(source);

    final Mapper<String, Citizen, String, Integer> mapper = new RegionPopulationMapper();
    final CombinerFactory<String, Integer, Integer> combinerFactory = new RegionPopulationCombinerFactory();
    final ReducerFactory<String, Integer, Integer> reducerFactory = new RegionPopulationReducerFactory();
    final Collator<Entry<String, Integer>, List<Entry<String, Integer>>> collator =
        new SortCollator<String, Integer>(Collections.reverseOrder(Entry.comparingByValue()));

    return job
        .mapper(mapper)
        .combiner(combinerFactory)
        .reducer(reducerFactory)
        .submit(collator);
  }

  @Override
  protected void getAClearClusterCollection(HazelcastInstance hazelcastInstance) {
    // TODO
  }

  @Override
  protected void addRecordToClusterCollection(CSVRecord csvRecord) {
    // TODO
  }

  @Override
  protected void internalRun(JobTracker jobTracker) {
    // TODO
  }

  public static class Builder extends AbstractQuery.Builder {
    @Override
    protected AbstractQuery build(final HazelcastInstance hazelcastInstance, final ClientArgs clientArgs) {
      return new RegionPopulationQuery(hazelcastInstance, clientArgs);
    }
  }
}

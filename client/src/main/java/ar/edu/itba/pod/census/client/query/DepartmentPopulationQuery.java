package ar.edu.itba.pod.census.client.query;

import ar.edu.itba.pod.census.client.CensusCSVRecords;
import ar.edu.itba.pod.census.collator.LimitedSortCollator;
import ar.edu.itba.pod.census.combiner.DepartmentPopulationCombinerFactory;
import ar.edu.itba.pod.census.config.SharedConfiguration;
import ar.edu.itba.pod.census.mapper.DepartmentPopulationMapper;
import ar.edu.itba.pod.census.model.Citizen;
import ar.edu.itba.pod.census.predicate.DepartmentPopulationFilter;
import ar.edu.itba.pod.census.reducer.DepartmentPopulationReducerFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Collator;
import com.hazelcast.mapreduce.CombinerFactory;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyPredicate;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.mapreduce.ReducerFactory;
import java.util.List;
import java.util.Map.Entry;

public final class DepartmentPopulationQuery {

  private DepartmentPopulationQuery() {
  }

  public static void fillData(final HazelcastInstance hazelcastInstance,
      final CensusCSVRecords records) {
    RegionOccupationQuery.fillData(hazelcastInstance, records);
  }

  public static ICompletableFuture<List<Entry<String, Integer>>> start(
      final HazelcastInstance hazelcastInstance, final String province, final int limit) {
    final JobTracker jobTracker = hazelcastInstance.getJobTracker(SharedConfiguration.TRACKER_NAME);
    final IMap<Long, Citizen> input = hazelcastInstance.getMap(SharedConfiguration.STRUCTURE_NAME);
    final KeyValueSource<Long, Citizen> source = KeyValueSource.fromMap(input);
    final Job<Long, Citizen> job = jobTracker.newJob(source);

    final KeyPredicate<Long> predicate =
        new DepartmentPopulationFilter(SharedConfiguration.STRUCTURE_NAME, province);
    final Mapper<Long, Citizen, String, Integer> mapper = new DepartmentPopulationMapper();
    final CombinerFactory<String, Integer, Integer> combinerFactory = new DepartmentPopulationCombinerFactory();
    final ReducerFactory<String, Integer, Integer> reducerFactory = new DepartmentPopulationReducerFactory();
    final Collator<Entry<String, Integer>, List<Entry<String, Integer>>> collator =
        new LimitedSortCollator<String, Integer>(limit, Entry.comparingByValue());

    return job
        .keyPredicate(predicate)
        .mapper(mapper)
        .combiner(combinerFactory)
        .reducer(reducerFactory)
        .submit(collator);
  }
}

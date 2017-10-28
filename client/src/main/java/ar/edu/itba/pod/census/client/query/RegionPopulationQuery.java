package ar.edu.itba.pod.census.client.query;

import ar.edu.itba.pod.census.collator.SortCollator;
import ar.edu.itba.pod.census.combiner.RegionPopulationCombinerFactory;
import ar.edu.itba.pod.census.config.SharedConfiguration;
import ar.edu.itba.pod.census.mapper.RegionPopulationMapper;
import ar.edu.itba.pod.census.model.Citizen;
import ar.edu.itba.pod.census.reducer.RegionPopulationReducerFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Collator;
import com.hazelcast.mapreduce.CombinerFactory;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.mapreduce.ReducerFactory;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public final class RegionPopulationQuery {

  private RegionPopulationQuery() {
  }

  public static void fillData(final HazelcastInstance hazelcastInstance,
      final Iterator<Citizen> citizenRecords) {
    final Map<Long, Citizen> map = hazelcastInstance.getMap(SharedConfiguration.MAP_NAME);

    long id = 0;
    while (citizenRecords.hasNext()) {
      final Citizen citizen = citizenRecords.next();
      map.put(id++, citizen);
    }
  }

  public static ICompletableFuture<List<Entry<String, Long>>> start(
      final HazelcastInstance hazelcastInstance) {
    final JobTracker jobTracker = hazelcastInstance
        .getJobTracker(SharedConfiguration.TRACKER_NAME);
    final IMap<Long, Citizen> map = hazelcastInstance.getMap(SharedConfiguration.MAP_NAME);
    final KeyValueSource<Long, Citizen> source = KeyValueSource.fromMap(map);
    final Job<Long, Citizen> job = jobTracker.newJob(source);

    final Mapper<Long, Citizen, String, Long> mapper = new RegionPopulationMapper();
    final CombinerFactory<String, Long, Long> combinerFactory = new RegionPopulationCombinerFactory();
    final ReducerFactory<String, Long, Long> reducerFactory = new RegionPopulationReducerFactory();
    final Collator<Entry<String, Long>, List<Entry<String, Long>>> collator =
        new SortCollator<String, Long>(Collections.reverseOrder(Entry.comparingByValue()));

    return job
        .mapper(mapper)
        .combiner(combinerFactory)
        .reducer(reducerFactory)
        .submit(collator);
  }
}

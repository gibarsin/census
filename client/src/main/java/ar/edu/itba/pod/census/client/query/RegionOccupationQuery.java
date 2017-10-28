package ar.edu.itba.pod.census.client.query;

import ar.edu.itba.pod.census.config.SharedConfiguration;
import ar.edu.itba.pod.census.mapper.RegionOccupationMapper;
import ar.edu.itba.pod.census.model.Citizen;
import ar.edu.itba.pod.census.reducer.RegionOccupationReducerFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.mapreduce.ReducerFactory;
import java.util.Iterator;
import java.util.Map;

public final class RegionOccupationQuery {

  private RegionOccupationQuery() {
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

  public static ICompletableFuture<Map<String, Double>> start(
      final HazelcastInstance hazelcastInstance, final int limit) {
    final JobTracker jobTracker = hazelcastInstance.getJobTracker(SharedConfiguration.TRACKER_NAME);
    final IMap<Long, Citizen> map = hazelcastInstance.getMap(SharedConfiguration.MAP_NAME);
    final KeyValueSource<Long, Citizen> source = KeyValueSource.fromMap(map);
    final Job<Long, Citizen> job = jobTracker.newJob(source);

    final Mapper<Long, Citizen, String, Boolean> mapper = new RegionOccupationMapper();
    final ReducerFactory<String, Boolean, Double> reducerFactory = new RegionOccupationReducerFactory();

    return job
        .mapper(mapper)
        .reducer(reducerFactory)
        .submit();
  }
}

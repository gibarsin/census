package ar.edu.itba.pod.census.client.query;

import ar.edu.itba.pod.census.client.CensusCSVRecords;
import ar.edu.itba.pod.census.client.CensusCSVRecords.Headers;
import ar.edu.itba.pod.census.collator.MinIntegerValueSortCollator;
import ar.edu.itba.pod.census.config.SharedConfiguration;
import ar.edu.itba.pod.census.mapper.SharedDepartmentCountMapper;
import ar.edu.itba.pod.census.model.Province;
import ar.edu.itba.pod.census.reducer.SharedDepartmentCountReducerFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Collator;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.mapreduce.ReducerFactory;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.commons.csv.CSVRecord;

public final class SharedDepartmentCountQuery {

  private SharedDepartmentCountQuery() {
  }

  public static void fillData(final HazelcastInstance hazelcastInstance,
      final CensusCSVRecords records) {
    final Map<String, Set<Province>> map = new HashMap<>();

    while (records.hasNext()) {
      final CSVRecord record = records.next();

      System.out.println(record.get(Headers.DEPARTMENT_NAME).trim() + " -> " + Province
          .fromString(record.get(Headers.PROVINCE_NAME)));

      map.computeIfAbsent(record.get(Headers.DEPARTMENT_NAME).trim(),
          k -> EnumSet.noneOf(Province.class))
          .add(Province.fromString(record.get(Headers.PROVINCE_NAME)));
    }

    for (final Entry<String, Set<Province>> entry : map.entrySet()) {
      System.out.println(entry.getKey() + " -> " + entry.getValue());
    }
  }

  public static ICompletableFuture<List<Entry<String, Integer>>> start(
      final HazelcastInstance hazelcastInstance, final int minValue) {
    final JobTracker jobTracker = hazelcastInstance.getJobTracker(SharedConfiguration.TRACKER_NAME);
    final IMap<String, Set<String>> map =
        hazelcastInstance.getMap(SharedConfiguration.STRUCTURE_NAME);
    final KeyValueSource<String, Set<String>> source = KeyValueSource.fromMap(map);
    final Job<String, Set<String>> job = jobTracker.newJob(source);

    final Mapper<String, Set<String>, String, Integer> mapper = new SharedDepartmentCountMapper();
    final ReducerFactory<String, Integer, Integer> reducerFactory = new SharedDepartmentCountReducerFactory();
    final Collator<Entry<String, Integer>, List<Entry<String, Integer>>> collator =
        new MinIntegerValueSortCollator<String>(minValue,
            Collections.reverseOrder(Entry.comparingByValue()));

    return job
        .mapper(mapper)
        .reducer(reducerFactory)
        .submit(collator);
  }
}

package ar.edu.itba.pod.census.client.query;

import ar.edu.itba.pod.census.client.CensusCSVRecords;
import ar.edu.itba.pod.census.client.CensusCSVRecords.Headers;
import ar.edu.itba.pod.census.collator.SortCollator;
import ar.edu.itba.pod.census.config.SharedConfiguration;
import ar.edu.itba.pod.census.mapper.HomeRegionMapper;
import ar.edu.itba.pod.census.model.Citizen;
import ar.edu.itba.pod.census.model.Region;
import ar.edu.itba.pod.census.reducer.HomesInRegionAverageReducerFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.Collator;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.mapreduce.ReducerFactory;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import org.apache.commons.csv.CSVRecord;

public class CitizensPerHomeInRegionQuery {

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

  public static ICompletableFuture<List<Entry<Region, Double>>> start(
      final HazelcastInstance hazelcastInstance) {
    final JobTracker jobTracker = hazelcastInstance
        .getJobTracker(SharedConfiguration.TRACKER_NAME);
    final IList<Citizen> input = hazelcastInstance.getList(SharedConfiguration.STRUCTURE_NAME);
    final KeyValueSource<String, Citizen> source = KeyValueSource.fromList(input);
    final Job<String, Citizen> job = jobTracker.newJob(source);

    final Mapper<String, Citizen, Region, Integer> mapper = new HomeRegionMapper();
    // TODO Check if I can use a set in a combiner
    final ReducerFactory<Region, Integer, Double> reducerFactory = new HomesInRegionAverageReducerFactory();
    final Collator<Entry<Region, Double>, List<Entry<Region, Double>>> collator =
        new SortCollator<Region, Double>(Collections.reverseOrder(Entry.comparingByValue()));

    return job
        .mapper(mapper)
        .reducer(reducerFactory)
        .submit(collator);
  }
}


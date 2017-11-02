package ar.edu.itba.pod.census.client.query;

import ar.edu.itba.pod.census.client.CensusCSVRecords;
import ar.edu.itba.pod.census.client.CensusCSVRecords.Headers;
import ar.edu.itba.pod.census.client.args.ClientArgs;
import ar.edu.itba.pod.census.config.SharedConfiguration;
import ar.edu.itba.pod.census.mapper.RegionOccupationMapper;
import ar.edu.itba.pod.census.model.Citizen;
import ar.edu.itba.pod.census.predicate.RegionOccupationFilter;
import ar.edu.itba.pod.census.reducer.RegionOccupationReducerFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyPredicate;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.mapreduce.ReducerFactory;

import java.io.PrintStream;
import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.commons.csv.CSVRecord;

public final class RegionOccupationQuery extends AbstractQuery {
  private RegionOccupationQuery(final HazelcastInstance hazelcastInstance, final ClientArgs clientArgs) {
    super(hazelcastInstance, clientArgs);
  }

  public static void fillData(final HazelcastInstance hazelcastInstance,
                              final CensusCSVRecords records) {
    final Map<Long, Citizen> map = hazelcastInstance.getMap(SharedConfiguration.STRUCTURE_NAME);

    long id = 0;
    while (records.hasNext()) {
      final CSVRecord record = records.next();

      map.put(id++, new Citizen(
          Integer.parseInt(record.get(Headers.EMPLOYMENT_STATUS).trim()),
          Integer.parseInt(record.get(Headers.HOME_ID).trim()),
          record.get(Headers.DEPARTMENT_NAME),
          record.get(Headers.PROVINCE_NAME)));
    }
  }

  public static ICompletableFuture<Map<String, BigDecimal>> start(
      final HazelcastInstance hazelcastInstance) {
    final JobTracker jobTracker = hazelcastInstance.getJobTracker(SharedConfiguration.TRACKER_NAME);
    final IMap<Long, Citizen> map = hazelcastInstance.getMap(SharedConfiguration.STRUCTURE_NAME);
    final KeyValueSource<Long, Citizen> source = KeyValueSource.fromMap(map);
    final Job<Long, Citizen> job = jobTracker.newJob(source);

    final KeyPredicate<Long> predicate = new RegionOccupationFilter(SharedConfiguration.STRUCTURE_NAME);
    final Mapper<Long, Citizen, String, Integer> mapper = new RegionOccupationMapper();
    final ReducerFactory<String, Integer, BigDecimal> reducerFactory = new RegionOccupationReducerFactory();

    return job
        .keyPredicate(predicate)
        .mapper(mapper)
        .reducer(reducerFactory)
        .submit();
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
      return new RegionOccupationQuery(hazelcastInstance, clientArgs);
    }
  }
}

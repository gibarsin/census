package ar.edu.itba.pod.census.client.query;

import ar.edu.itba.pod.census.client.CensusCSVRecords;
import ar.edu.itba.pod.census.client.CensusCSVRecords.Headers;
import ar.edu.itba.pod.census.client.args.ClientArgs;
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

import java.io.PrintStream;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

import org.apache.commons.csv.CSVRecord;

public final class CitizensPerHomeInRegionQuery extends AbstractQuery {
  private CitizensPerHomeInRegionQuery(HazelcastInstance hazelcastInstance, ClientArgs clientArgs) {
    super(hazelcastInstance, clientArgs);
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

  public static ICompletableFuture<List<Entry<Region, BigDecimal>>> start(
      final HazelcastInstance hazelcastInstance) {
    final JobTracker jobTracker = hazelcastInstance
        .getJobTracker(SharedConfiguration.TRACKER_NAME);
    final IList<Citizen> input = hazelcastInstance.getList(SharedConfiguration.STRUCTURE_NAME);
    final KeyValueSource<String, Citizen> source = KeyValueSource.fromList(input);
    final Job<String, Citizen> job = jobTracker.newJob(source);

    final Mapper<String, Citizen, Region, Integer> mapper = new HomeRegionMapper();
    // TODO Check if I can use a set in a combiner
    final ReducerFactory<Region, Integer, BigDecimal> reducerFactory = new HomesInRegionAverageReducerFactory();
    final Collator<Entry<Region, BigDecimal>, List<Entry<Region, BigDecimal>>> collator =
        new SortCollator<Region, BigDecimal>(Collections.reverseOrder(Entry.comparingByValue()));

    return job
        .mapper(mapper)
        .reducer(reducerFactory)
        .submit(collator);
  }

  public static class Builder extends AbstractQuery.Builder {
    @Override
    protected AbstractQuery build(final HazelcastInstance hazelcastInstance, final ClientArgs clientArgs) {
      return new CitizensPerHomeInRegionQuery(hazelcastInstance, clientArgs);
    }
  }
}


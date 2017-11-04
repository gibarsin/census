package ar.edu.itba.pod.census.client.query;

import ar.edu.itba.pod.census.client.CensusCSVRecords.Headers;
import ar.edu.itba.pod.census.client.args.ClientArgs;
import ar.edu.itba.pod.census.collator.SortCollator;
import ar.edu.itba.pod.census.combiner.CitizensPerHomeByRegionCombinerFactory;
import ar.edu.itba.pod.census.config.SharedConfiguration;
import ar.edu.itba.pod.census.mapper.CitizensPerHomeByRegionMapper;
import ar.edu.itba.pod.census.model.Container;
import ar.edu.itba.pod.census.model.Region;
import ar.edu.itba.pod.census.reducer.CitizensPerHomeByRegionReducerFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.*;

import java.io.PrintStream;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

@SuppressWarnings("deprecation")
// Intentionally as we are using deprecated Hazelcast features
public final class CitizensPerHomeByRegionQuery extends AbstractQuery {
  private IList<Container> input;
  private ReducingSubmittableJob<String, Region, BigDecimal> mapReducerJob;
  private Collator<Entry<Region, BigDecimal>, List<Entry<Region, BigDecimal>>> collator;
  private List<Entry<Region, BigDecimal>> jobResult;

  private CitizensPerHomeByRegionQuery(final HazelcastInstance hazelcastInstance, final ClientArgs clientArgs) {
    super(hazelcastInstance, clientArgs);
  }

  @Override
  protected void pickAClearClusterCollection(final HazelcastInstance hazelcastInstance) {
    input = hazelcastInstance.getList(SharedConfiguration.STRUCTURE_NAME);
    input.clear();
  }

  @Override
  protected void addRecordToClusterCollection(final String[] csvRecord) {
    input.add(new Container(-1,
            Integer.parseInt(csvRecord[Headers.HOME_ID.getColumn()].trim()),
            "",
            csvRecord[Headers.PROVINCE_NAME.getColumn()]
    ));
  }

  @Override
  protected void prepareJobResources(final JobTracker jobTracker) {
    // Create the custom job
    final KeyValueSource<String, Container> source = KeyValueSource.fromList(input);
    final Job<String, Container> job = jobTracker.newJob(source);

    // Prepare the map reduce job to be submitted
    mapReducerJob = job.mapper(new CitizensPerHomeByRegionMapper())
            .combiner(new CitizensPerHomeByRegionCombinerFactory())
            .reducer(new CitizensPerHomeByRegionReducerFactory());

    // Prepare the collator to post-process the job's result
    // Compiler complains if we do not set this explicitly
    //noinspection Convert2Diamond
    collator = new SortCollator<Region, BigDecimal>(Collections.reverseOrder(Entry.comparingByValue()));
  }

  @Override
  protected void submitJob() throws ExecutionException, InterruptedException {
    jobResult = mapReducerJob.submit(collator).get();
  }

  @Override
  protected void processJobResult(final PrintStream output) {
    jobResult.forEach(entry -> output.println(entry.getKey() + "," + entry.getValue()));
  }

  public static class Builder extends AbstractQuery.Builder {
    @Override
    protected AbstractQuery build(final HazelcastInstance hazelcastInstance, final ClientArgs clientArgs) {
      return new CitizensPerHomeByRegionQuery(hazelcastInstance, clientArgs);
    }
  }
}


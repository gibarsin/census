package ar.edu.itba.pod.census.client.query;

import ar.edu.itba.pod.census.client.CensusCSVRecords.Headers;
import ar.edu.itba.pod.census.client.args.ClientArgs;
import ar.edu.itba.pod.census.collator.SortCollator;
import ar.edu.itba.pod.census.combiner.RegionOccupationCombinerFactory;
import ar.edu.itba.pod.census.config.SharedConfiguration;
import ar.edu.itba.pod.census.mapper.RegionOccupationMapper;
import ar.edu.itba.pod.census.model.Container;
import ar.edu.itba.pod.census.model.Region;
import ar.edu.itba.pod.census.reducer.RegionOccupationReducerFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.*;

import java.io.PrintStream;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public final class RegionOccupationQuery extends AbstractQuery {

  private final List<Container> localInput = new LinkedList<>();
  private IList<Container> remoteInput;
  private ReducingSubmittableJob<String, Region, BigDecimal> mapReducerJob;
  private Collator<Map.Entry<Region, BigDecimal>, List<Map.Entry<Region, BigDecimal>>> collator;
  private List<Map.Entry<Region, BigDecimal>> jobResult;

  private RegionOccupationQuery(final HazelcastInstance hazelcastInstance, final ClientArgs clientArgs) {
    super(hazelcastInstance, clientArgs);
  }

  @Override
  protected void pickAClearClusterCollection(final HazelcastInstance hazelcastInstance) {
    remoteInput = hazelcastInstance.getList(SharedConfiguration.STRUCTURE_NAME);
    remoteInput.clear();
  }

  @Override
  protected void addRecordToClusterCollection(final String[] csvRecord) {
    localInput.add(new Container(
            Integer.parseInt(csvRecord[Headers.EMPLOYMENT_STATUS.getColumn()].trim()),
            -1,
            "",
            csvRecord[Headers.PROVINCE_NAME.getColumn()]
    ));
  }

  @Override
  protected void submitAllRecordsToCluster() {
    remoteInput.addAll(localInput);
  }

  @Override
  protected void prepareJobResources(final JobTracker jobTracker) {
    // Create the custom job
    final KeyValueSource<String, Container> source = KeyValueSource.fromList(remoteInput);
    final Job<String, Container> job = jobTracker.newJob(source);

    // Prepare the map reduce job to be submitted
    mapReducerJob = job.mapper(new RegionOccupationMapper())
            .combiner(new RegionOccupationCombinerFactory())
            .reducer(new RegionOccupationReducerFactory());

    // Prepare the collator to post-process the job's result
    // Compiler complains if we do not set this explicitly
    //noinspection Convert2Diamond
    collator = new SortCollator<Region, BigDecimal>(Collections.reverseOrder(Map.Entry.comparingByValue()));
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
      return new RegionOccupationQuery(hazelcastInstance, clientArgs);
    }
  }
}

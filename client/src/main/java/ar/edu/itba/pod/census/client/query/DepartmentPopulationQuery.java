package ar.edu.itba.pod.census.client.query;

import ar.edu.itba.pod.census.client.CensusCSVRecords;
import ar.edu.itba.pod.census.client.args.ClientArgs;
import ar.edu.itba.pod.census.collator.LimitedSortCollator;
import ar.edu.itba.pod.census.combiner.NoKeyAdderCombinerFactory;
import ar.edu.itba.pod.census.config.SharedConfiguration;
import ar.edu.itba.pod.census.mapper.DepartmentPopulationMapper;
import ar.edu.itba.pod.census.model.Container;
import ar.edu.itba.pod.census.reducer.NoKeyAdderReducerFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.*;

import java.io.PrintStream;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

@SuppressWarnings("deprecation")
// Intentionally as we are using deprecated Hazelcast features
public final class DepartmentPopulationQuery extends AbstractQuery {
  private final int requiredN;
  private final String requiredProvince;

  private IList<Container> input;
  private ReducingSubmittableJob<String, String, Integer> mapReducerJob;
  private Collator<Entry<String, Integer>, List<Entry<String, Integer>>> collator;
  private List<Entry<String, Integer>> jobResult;

  private DepartmentPopulationQuery(final HazelcastInstance hazelcastInstance, final ClientArgs clientArgs) {
    super(hazelcastInstance, clientArgs);
    this.requiredN = clientArgs.getN();
    this.requiredProvince = clientArgs.getProvince();
  }

  @Override
  protected void pickAClearClusterCollection(final HazelcastInstance hazelcastInstance) {
    input = hazelcastInstance.getList(SharedConfiguration.STRUCTURE_NAME);
    input.clear();
  }

  @Override
  protected void addRecordToClusterCollection(final String[] csvRecord) {
    input.add(new Container(-1,-1,
            csvRecord[CensusCSVRecords.Headers.DEPARTMENT_NAME.getColumn()],
            csvRecord[CensusCSVRecords.Headers.PROVINCE_NAME.getColumn()]
    ));
  }

  @Override
  protected void prepareJobResources(final JobTracker jobTracker) {
    // Create the custom job
    final KeyValueSource<String, Container> source = KeyValueSource.fromList(input);
    final Job<String, Container> job = jobTracker.newJob(source);

    // Prepare the map reduce job to be submitted
    mapReducerJob = job.mapper(new DepartmentPopulationMapper(requiredProvince))
            .combiner(new NoKeyAdderCombinerFactory())
            .reducer(new NoKeyAdderReducerFactory());

    // Prepare the collator to post-process the job's result
    // Compiler complains if we do not set this explicitly
    //noinspection Convert2Diamond
    collator = new LimitedSortCollator<String, Integer>(requiredN, Entry.comparingByValue());
  }

  @Override
  protected void submitJob() throws ExecutionException, InterruptedException {
    jobResult = mapReducerJob.submit(collator).get();
  }

  @Override
  protected void processJobResult(final PrintStream output) { // TODO: Same as query 1
    jobResult.forEach(entry -> output.println(entry.getKey() + "," + entry.getValue()));
  }

  public static class Builder extends AbstractQuery.Builder {
    @Override
    protected AbstractQuery build(final HazelcastInstance hazelcastInstance, final ClientArgs clientArgs) {
      return new DepartmentPopulationQuery(hazelcastInstance, clientArgs);
    }
  }
}

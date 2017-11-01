package ar.edu.itba.pod.census.client.query;

import ar.edu.itba.pod.census.client.CensusCSVRecords;
import ar.edu.itba.pod.census.client.args.ClientArgs;
import ar.edu.itba.pod.census.client.exception.InputFileErrorException;
import ar.edu.itba.pod.census.client.exception.QueryFailedException;
import ar.edu.itba.pod.census.config.SharedConfiguration;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.mapreduce.JobTracker;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

@SuppressWarnings("deprecation")
// Intentionally as we are using deprecated Hazelcast features
public abstract class AbstractQuery implements IQuery {
  private final static Logger LOGGER = LoggerFactory.getLogger(AbstractQuery.class);
  private final HazelcastInstance hazelcastInstance;
  private final String inPath;

  @SuppressWarnings("WeakerAccess")
  // Intentionally left protected instead of package-private because of possible inheritance in other packages
  protected AbstractQuery(final HazelcastInstance hazelcastInstance, final ClientArgs clientArgs) {
    this.hazelcastInstance = hazelcastInstance;
    // TODO: pass the client args so each query can grab the desired parameters
    this.inPath = clientArgs.getInPath();
  }

  @Override
  public void run() throws QueryFailedException, InputFileErrorException {
    fillData(inPath);
    final JobTracker jobTracker = hazelcastInstance.getJobTracker(SharedConfiguration.TRACKER_NAME);
    // Prepare the Map Reduce Job
    buildMapReduceJob(jobTracker);
    // IMPORTANT: Prior to the log so as not to affect the logging time (which is the one considered by professors)
    final long start = System.currentTimeMillis();
    try {
      // Log starting
      LOGGER.info("Inicio del trabajo map/reduce");
      submitJob();
    } catch (ExecutionException | InterruptedException e) {
      LOGGER.error("Job failed", e);
      throw new QueryFailedException("Job failed");
    } finally {
      // Log end
      LOGGER.info("Fin del trabajo map/reduce");
      // IMPORTANT: Following the log so as not to affect the logging time (which is the one considered by professors)
      final long end = System.currentTimeMillis();
      LOGGER.debug("Tiempo de trabajo entre ambos logs (aproximadamente): {} ms.", end - start);
    }
    processJobResult();
  }

  private void fillData(final String inPath) throws InputFileErrorException {
    getAClearClusterCollection(hazelcastInstance);
    // IMPORTANT: Prior to the log so as not to affect the logging time (which is the one considered by professors)
    final long start = System.currentTimeMillis();
    // Load time includes both the file load and the cluster collection load
    LOGGER.info("Inicio de la lectura del archivo");
    try (CensusCSVRecords csvRecords = CensusCSVRecords.open(inPath)) {
      populateClusterCollection(csvRecords);
    } catch (final IOException exception) {
      LOGGER.error("Could not open/read input file", exception);
      throw new InputFileErrorException("There was an error while trying to open/read the input file");
    }
    LOGGER.info("Fin de lectura del archivo");
    // IMPORTANT: Following the log so as not to affect the logging time (which is the one considered by professors)
    final long end = System.currentTimeMillis();
    LOGGER.debug("Tiempo de lectura entre ambos logs (aproximadamente): {} ms.", end - start);
  }

  private void populateClusterCollection(CensusCSVRecords csvRecords) {
    while (csvRecords.hasNext()) {
      addRecordToClusterCollection(csvRecords.next());
    }
  }

  /**
   * Get the needed cluster collection from the given {@code hazelcastInstance}.
   * <p>
   * <b>Note that the <i>clear</i> section of this name means that it must be ensure that the collection has no previous
   * items, so implementations have to take care of this clean up.</b>
   *
   * @param hazelcastInstance The hazelcast instance from where to require the needed cluster collection
   */
  protected abstract void getAClearClusterCollection(final HazelcastInstance hazelcastInstance);

  /**
   * Fill the cluster collection already initialized by the {@code getAClearClusterCollection} with the given
   * {@code csvRecord} as needed.
   *
   * @param csvRecord The csv record to be added to the cluster collection
   */
  protected abstract void addRecordToClusterCollection(CSVRecord csvRecord);

  /**
   * Internally initialize all the needed stuff to perform the job submission.
   * <p>
   * This method is not considered in the performance measure.
   * <p>
   * This is the step 1/3 of the run process.
   *
   * @param jobTracker The job tracker to be used to create the job to be submitted later
   */
  protected abstract void buildMapReduceJob(final JobTracker jobTracker);

  /**
   * Submit the already prepared job and internally store its result to lately process it.
   * <p>
   * This method is the only one considered in the performance measure.
   * <p>
   * This is the step 2/3 of the run process.
   * @throws ExecutionException if the job computation threw an exception
   * @throws InterruptedException if the job's thread was interrupted while waiting its response
   */
  protected abstract void submitJob() throws ExecutionException, InterruptedException;

  /**
   * Internally initialize all the needed stuff to perform the job submission.
   * <p>
   * This method is not considered in the performance measure.
   * <p>
   * This is the step 1/3 of the run process.
   */
  protected abstract void processJobResult();

  public static abstract class Builder {
    private HazelcastInstance hazelcastInstance;
    private ClientArgs clientArgs;

    public Builder setHazelcastInstance(HazelcastInstance hazelcastInstance) {
      this.hazelcastInstance = hazelcastInstance;
      return this;
    }

    public Builder setClientArgs(ClientArgs clientArgs) {
      this.clientArgs = clientArgs;
      return this;
    }

    public AbstractQuery build() {
      return build(Objects.requireNonNull(hazelcastInstance), Objects.requireNonNull(clientArgs));
    }

    /**
     * Create a new instance of an AbstractQuery type with the validated arguments
     * @param hazelcastInstance The non-null hazelcast instance
     * @param clientArgs The non-null and validated client args
     */
    protected abstract AbstractQuery build(HazelcastInstance hazelcastInstance, ClientArgs clientArgs);
  }
}

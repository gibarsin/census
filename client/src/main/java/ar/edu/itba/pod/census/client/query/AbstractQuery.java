package ar.edu.itba.pod.census.client.query;

import ar.edu.itba.pod.census.client.CensusCSVRecords;
import ar.edu.itba.pod.census.client.args.ClientArgs;
import ar.edu.itba.pod.census.config.SharedConfiguration;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.mapreduce.JobTracker;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public abstract class AbstractQuery implements IQuery {
  private final static Logger LOGGER = LoggerFactory.getLogger(AbstractQuery.class);
  private final HazelcastInstance hazelcastInstance;

  public AbstractQuery(final HazelcastInstance hazelcastInstance, final ClientArgs clientArgs) {
    this.hazelcastInstance = hazelcastInstance;
    fillData(clientArgs);
  }

  private void fillData(final ClientArgs clientArgs) {
    getAClearClusterCollection(hazelcastInstance);
    // IMPORTANT: Prior to the log so as not to affect the logging time (which is the one considered by professors)
    final long start = System.currentTimeMillis();
    // Load time includes both the file load and the cluster collection load
    LOGGER.info("Inicio de la lectura del archivo");
    try (CensusCSVRecords csvRecords = CensusCSVRecords.open(clientArgs.getInPath())) {
      populateClusterCollection(csvRecords);
    } catch (final IOException exception) {
      System.err.println("There was an error while trying to open/read the input file.");
      LOGGER.error("Could not open/read input file", exception);
      System.exit(1);
    }
    LOGGER.info("Fin de lectura del archivo");
    // IMPORTANT: Following the log so as not to affect the logging time (which is the one considered by professors)
    final long end = System.currentTimeMillis();
    LOGGER.debug("Tiempo de lectura entre ambos logs (aproximadamente): {0} ms.", end - start);
  }

  private void populateClusterCollection(CensusCSVRecords csvRecords) {
    while (csvRecords.hasNext()) {
      addRecordToClusterCollection(csvRecords.next());
    }
  }

  @Override
  public void run() {
    final JobTracker jobTracker = hazelcastInstance.getJobTracker(SharedConfiguration.TRACKER_NAME);
    internalRun(jobTracker);
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
   * Create & submit the job query represented by the current class using the given {@code jobTracker}.
   *
   * @param jobTracker The job tracker used to create the job to be submitted
   */
  protected abstract void internalRun(JobTracker jobTracker);
}

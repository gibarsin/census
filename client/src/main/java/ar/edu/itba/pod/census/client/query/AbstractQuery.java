package ar.edu.itba.pod.census.client.query;

import ar.edu.itba.pod.census.client.args.ClientArgs;
import ar.edu.itba.pod.census.client.exception.InputFileErrorException;
import ar.edu.itba.pod.census.client.exception.OutputFileErrorException;
import ar.edu.itba.pod.census.client.exception.QueryFailedException;
import ar.edu.itba.pod.census.config.SharedConfiguration;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.filter.ThresholdFilter;
import ch.qos.logback.core.ConsoleAppender;
import ch.qos.logback.core.FileAppender;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.mapreduce.JobTracker;

import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;

import java.io.*;
import java.nio.file.Files;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

// Intentionally as we are using deprecated Hazelcast features
@SuppressWarnings("deprecation")
public abstract class AbstractQuery implements IQuery {
  private static final String CSV_SPLITTER = ",";
  private final HazelcastInstance hazelcastInstance;
  private final String inPath;
  private final String outPath;
  private final Logger logger;

  // Intentionally left protected instead of package-private because of possible inheritance in other packages
  @SuppressWarnings("WeakerAccess")
  protected AbstractQuery(final HazelcastInstance hazelcastInstance, final ClientArgs clientArgs) {
    this.hazelcastInstance = hazelcastInstance;
    this.inPath = clientArgs.getInPath();
    this.outPath = clientArgs.getOutPath();
    // Set up a logger dynamically
    this.logger = createLogger(clientArgs.getTimeOutPath(), clientArgs.getDebug());
  }

  // Thanks:
  // - https://stackoverflow.com/questions/16910955/programmatically-configure-logback-appender
  // - https://stackoverflow.com/questions/1324053/configure-log4j-to-log-to-custom-file-at-runtime
  // - https://stackoverflow.com/questions/5448673/slf4j-logback-how-to-configure-loggers-in-runtime
  // - https://logback.qos.ch/manual/layouts.html
  private Logger createLogger(final String timeOutPath, final boolean debug) {
    final LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();

    final PatternLayoutEncoder logEncoder = new PatternLayoutEncoder();
    logEncoder.setContext(loggerContext);
    logEncoder.setPattern("%-6date{dd/MM/YYYY HH:mm:ss:SSSS} %level  [%thread] %logger{36} \\(%F:%L\\) - %msg%n");
    logEncoder.start();

    final ThresholdFilter filter = new ThresholdFilter();
    filter.setLevel("INFO");
    filter.start();

    final FileAppender<ch.qos.logback.classic.spi.ILoggingEvent> fileAppender = new FileAppender<>();
    fileAppender.addFilter(filter);
    fileAppender.setContext(loggerContext);
    fileAppender.setName(timeOutPath);
    fileAppender.setEncoder(logEncoder);
    fileAppender.setAppend(true);
    fileAppender.setFile(timeOutPath);
    fileAppender.start();

    final Logger logger = (Logger) LoggerFactory.getLogger(AbstractQuery.class.getSimpleName());
    logger.setAdditive(false);
    logger.setLevel(Level.DEBUG);
    logger.addAppender(fileAppender);

    if (debug) {
      final ConsoleAppender<ch.qos.logback.classic.spi.ILoggingEvent> logConsoleAppender = new ConsoleAppender<>();
      logConsoleAppender.setContext(loggerContext);
      logConsoleAppender.setName("Console");
      logConsoleAppender.setEncoder(logEncoder);
      logConsoleAppender.start();

      logger.addAppender(logConsoleAppender);
    }

    return logger;
  }

  @Override
  public void run() throws QueryFailedException, InputFileErrorException, OutputFileErrorException {
    fillData(inPath);
    final JobTracker jobTracker = hazelcastInstance.getJobTracker(SharedConfiguration.TRACKER_NAME);
    // Prepare the Map Reduce Job
    prepareJobResources(jobTracker);
    // IMPORTANT: Prior to the log so as not to affect the logging time (which is the one considered by professors)
    final long start = System.currentTimeMillis();
    try {
      // Log starting
      logger.info("Inicio del trabajo map/reduce");
      submitJob();
    } catch (ExecutionException | InterruptedException e) {
      logger.error("Job failed", e);
      throw new QueryFailedException("Job failed");
    } finally {
      // Log end
      logger.info("Fin del trabajo map/reduce");
      // IMPORTANT: Following the log so as not to affect the logging time (which is the one considered by professors)
      final long end = System.currentTimeMillis();
      logger.debug("Tiempo de trabajo entre ambos logs (aproximadamente): {} ms.", end - start);
    }
    final PrintStream output = loadOutputStream(outPath);
    processJobResult(output);
    output.close();
  }

  private PrintStream loadOutputStream(final String outPath) throws OutputFileErrorException {
    final PrintStream output;
    try {
      final File file = new File(outPath);
      Files.deleteIfExists(file.toPath());
      output = new PrintStream(new FileOutputStream(outPath, false));
    } catch (final IOException e) {
      final String msg = "Could not delete/open/write output file";
      logger.error(msg, e);
      throw new OutputFileErrorException(msg);
    }
    return output;
  }

  private void fillData(final String inPath) throws InputFileErrorException {
    pickAClearClusterCollection(hazelcastInstance);
    // IMPORTANT: Prior to the log so as not to affect the logging time (which is the one considered by professors)
    final long start = System.currentTimeMillis();
    // Load time includes both the file load and the cluster collection load
    logger.info("Inicio de la lectura del archivo");
    try (BufferedReader br = new BufferedReader(new FileReader(inPath))) {
      String line;
      while ((line = br.readLine()) != null) {
        addRecordToClusterCollection(line.split(CSV_SPLITTER));
      }
    } catch (final IOException exception) {
      logger.error("Could not open/read input file", exception);
      throw new InputFileErrorException("There was an error while trying to open/read the input file");
    }
    logger.info("Fin de lectura del archivo");
    // IMPORTANT: Following the log so as not to affect the logging time (which is the one considered by professors)
    final long end = System.currentTimeMillis();
    logger.debug("Tiempo de lectura entre ambos logs (aproximadamente): {} ms.", end - start);
  }

  /**
   * Pick the needed cluster collection from the given {@code hazelcastInstance}.
   * <p>
   * <b>Note that the <i>clear</i> section of this name means that it must be ensure that the collection has no previous
   * items, so implementations have to take care of this clean up.</b>
   *
   * @param hazelcastInstance The hazelcast instance from where to require the needed cluster collection
   */
  protected abstract void pickAClearClusterCollection(final HazelcastInstance hazelcastInstance);

  /**
   * Fill the cluster collection already initialized by the {@code pickAClearClusterCollection} with the given
   * {@code csvRecord} as needed.
   *
   * @param csvRecord The csv record to be added to the cluster collection
   */
  protected abstract void addRecordToClusterCollection(String[] csvRecord);

  /**
   * Internally initialize all the needed stuff to perform the job submission.
   * <p>
   * This method is not considered in the performance measure.
   * <p>
   * This is the step 1/3 of the run process.
   *
   * @param jobTracker The job tracker to be used to create the job to be submitted later
   */
  protected abstract void prepareJobResources(final JobTracker jobTracker);

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
   * @param output The output stream were the processed job result should be written
   */
  protected abstract void processJobResult(final PrintStream output);

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

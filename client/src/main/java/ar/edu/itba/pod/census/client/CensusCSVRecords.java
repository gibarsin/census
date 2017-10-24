package ar.edu.itba.pod.census.client;

import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CensusCSVRecords implements Iterator<CSVRecord> {

  private final static Logger logger = LoggerFactory.getLogger(CensusCSVRecords.class);

  private final static String RESOURCES_PATH = System.getProperty("user.dir")
      + "/client/src/main/resources/";

  private final String csvPath;
  private Iterator<CSVRecord> recordsIterator;

  /* package-private */
  CensusCSVRecords(final String csvFileName) {
    csvPath = RESOURCES_PATH + csvFileName;
    parseFile();
  }

  private void parseFile() {
    try (FileReader in = new FileReader(csvPath)) {
      parseCSVFileReader(in);
    } catch (final IOException e) {
      logger.warn("An IOException occurred while attempting to read CSV file: {}", csvPath);
    }
  }

  private void parseCSVFileReader(final FileReader in) {
    try (CSVParser parser = CSVFormat.DEFAULT.parse(in)) {
      recordsIterator = parser.getRecords().iterator();
    } catch (final IOException e) {
      logger.warn("An IOException occurred while parsing the CSV file: {}", csvPath);
    }
  }

  @Override
  public boolean hasNext() {
    return recordsIterator.hasNext();
  }

  @Override
  public CSVRecord next() {
    return recordsIterator.next();
  }
}

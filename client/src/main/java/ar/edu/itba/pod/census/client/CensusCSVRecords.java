package ar.edu.itba.pod.census.client;

import ar.edu.itba.pod.census.model.Citizen;
import java.io.Closeable;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CensusCSVRecords implements Iterator<Citizen>, Closeable, AutoCloseable {

  private final static Logger LOGGER = LoggerFactory.getLogger(CensusCSVRecords.class);

  private final Reader inFile;
  private final CSVParser parser;
  private final Iterator<CSVRecord> recordsIterator;

  private CensusCSVRecords(final String csvFilePath) throws IOException {
    this.inFile = new FileReader(csvFilePath);
    this.parser = CSVFormat.DEFAULT.withHeader(Headers.class).parse(inFile);
    this.recordsIterator = parser.iterator();
  }

  public static CensusCSVRecords open(final String csvFilePath) throws IOException {
    return new CensusCSVRecords(csvFilePath);
  }

  @Override
  public boolean hasNext() {
    return recordsIterator.hasNext();
  }

  @Override
  public Citizen next() {
    final CSVRecord record = recordsIterator.next();

    return new Citizen(
        Integer.parseInt(record.get(Headers.EMPLOYMENT_STATUS).trim()),
        Integer.parseInt(record.get(Headers.HOME_ID).trim()),
        record.get(Headers.DEPARTMENT_NAME),
        record.get(Headers.PROVINCE_NAME));
  }

  @Override
  public void remove() {
    recordsIterator.remove();
  }

  @Override
  public void close() throws IOException {
    parser.close();
    inFile.close();
  }

  public enum Headers {
    EMPLOYMENT_STATUS, HOME_ID, DEPARTMENT_NAME, PROVINCE_NAME
  }
}

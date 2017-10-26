package ar.edu.itba.pod.census.client;

import java.io.Closeable;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;
import java.util.function.Consumer;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CensusCSVRecords implements Iterator<CSVRecord>, Closeable, AutoCloseable {

  private final static Logger LOGGER = LoggerFactory.getLogger(CensusCSVRecords.class);

  private final Reader inFile;
  private final CSVParser parser;
  private final Iterator<CSVRecord> recordsIterator;

  /* package-private */ CensusCSVRecords(final String csvFilePath) throws IOException {

    this.inFile = new FileReader(csvFilePath);
    this.parser = CSVFormat.DEFAULT.withHeader(Headers.class).parse(inFile);
    this.recordsIterator = parser.iterator();
  }

  @Override
  public boolean hasNext() {
    return recordsIterator.hasNext();
  }

  @Override
  public CSVRecord next() {
    return recordsIterator.next();
  }

  @Override
  public void remove() {
    recordsIterator.remove();
  }

  @Override
  public void forEachRemaining(final Consumer<? super CSVRecord> action) {
    recordsIterator.forEachRemaining(action);
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

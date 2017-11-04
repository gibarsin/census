package ar.edu.itba.pod.census.client;

import java.io.Closeable;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public final class CensusCSVRecords implements Iterator<CSVRecord>, Closeable, AutoCloseable {
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
  public CSVRecord next() {
    return recordsIterator.next();
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

  // IMPORTANT: Do not alter the order of the headers values
  // TODO: Migrate this to indexes, remove the above behavior and leave only this enum
  public enum Headers {
    EMPLOYMENT_STATUS(0), HOME_ID(1), DEPARTMENT_NAME(2), PROVINCE_NAME(3);

    private final int column;

    Headers(final int column) {
      this.column = column;
    }

    public int getColumn() {
      return column;
    }
  }
}

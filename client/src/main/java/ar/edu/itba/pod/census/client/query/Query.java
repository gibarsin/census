package ar.edu.itba.pod.census.client.query;

import ar.edu.itba.pod.census.client.exception.InputFileErrorException;
import ar.edu.itba.pod.census.client.exception.OutputFileErrorException;
import ar.edu.itba.pod.census.client.exception.QueryFailedException;

public interface Query {
  /**
   * Run the query represented by the class that implements this interface
   *
   * @throws QueryFailedException if the query computation threw an exception
   *                              or was suddenly interrupted while waiting response
   * @throws InputFileErrorException if the input file could not be opened or read
   * @throws OutputFileErrorException if the output file could not be deleted, opened or written
   */
  void run() throws QueryFailedException, InputFileErrorException, OutputFileErrorException;
}

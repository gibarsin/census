package ar.edu.itba.pod.census.client.query;

import ar.edu.itba.pod.census.client.exception.InputFileErrorException;
import ar.edu.itba.pod.census.client.exception.OutputFileErrorException;
import ar.edu.itba.pod.census.client.exception.QueryFailedException;

public interface IQuery {
  /**
   * Run the query represented by the class that implements this interface
   */
  // TODO: document exceptions
  void run() throws QueryFailedException, InputFileErrorException, OutputFileErrorException;
}

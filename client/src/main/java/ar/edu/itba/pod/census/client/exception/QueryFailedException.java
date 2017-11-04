package ar.edu.itba.pod.census.client.exception;

public class QueryFailedException extends Exception {
  public QueryFailedException(final String msg) {
    super(msg);
  }
}

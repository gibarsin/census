package ar.edu.itba.pod.census.client.args;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;

public final class QueryArgValidator implements IParameterValidator {
  private static final int QUERY_MAX = 7;
  private static final int QUERY_MIN = 1;

  @Override
  public void validate(final String name, final String value) throws ParameterException {
    final int query = Integer.parseInt(value);

    if (query < QUERY_MIN || query > QUERY_MAX) {
      throw new ParameterException("Query should be between " + QUERY_MIN +
          "  (inclusive) and " + QUERY_MAX + " (inclusive)");
    }
  }
}

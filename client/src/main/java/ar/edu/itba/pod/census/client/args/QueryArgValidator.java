package ar.edu.itba.pod.census.client.args;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;

public final class QueryArgValidator implements IParameterValidator {

  @Override
  public void validate(final String name, final String value) throws ParameterException {
    final Integer query = Integer.parseInt(value);

    if (1 > query || 7 < query) {
      throw new ParameterException("Query should be between 1 (inclusive) and 7 (inclusive)");
    }
  }
}

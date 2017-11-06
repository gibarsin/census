package ar.edu.itba.pod.census.client.args;

import ar.edu.itba.pod.census.model.Province;
import com.beust.jcommander.ParameterException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class CustomArgsValidator {
  private static final List<Integer> QUERIES_THAT_REQUIRE_N = Arrays.asList(2, 6, 7);
  private static final List<Integer> QUERIES_THAT_REQUIRE_PROVINCE = Collections.singletonList(2);

  public static void validate(final ClientArgs clientArgs) throws ParameterException {
    final int query = clientArgs.getQuery();

    validateN(query, clientArgs.getN());
    validateProvince(query, clientArgs.getProvince());
  }

  private static void validateN(final int query, final Integer n) {
    if (QUERIES_THAT_REQUIRE_N.contains(query) && (n == null || n <= 0)) {
      throw new ParameterException("N should be defined and greater than 0 for this query");
    } else if (!QUERIES_THAT_REQUIRE_N.contains(query) && n != null) {
      throw new ParameterException("N should be undefined for this query");
    }
  }

  private static void validateProvince(final int query, final String province) {
    if (QUERIES_THAT_REQUIRE_PROVINCE.contains(query)) {
      if (province == null) {
        throw new ParameterException("Province should be defined for this query");
      } else {
        try {
          Province.fromString(province);
        } catch (final IllegalArgumentException e) {
          throw new ParameterException(e.getMessage());
        }
      }
    } else if (!QUERIES_THAT_REQUIRE_PROVINCE.contains(query) && province != null) {
        throw new ParameterException("Province should be undefined for this query");
    }
  }
}

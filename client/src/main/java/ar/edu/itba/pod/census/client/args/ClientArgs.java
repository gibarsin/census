package ar.edu.itba.pod.census.client.args;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Parameters(separators = "=")
@SuppressWarnings({"UnusedDeclaration"}) // JCommander populates all the private fields
public final class ClientArgs {

  @Parameter(names = "-Daddresses", required = true)
  private List<String> addresses = new ArrayList<>();

  @Parameter(names = "-Dquery", validateWith = QueryArgValidator.class, required = true)
  private Integer query;

  @Parameter(names = "-DinPath", required = true)
  private String inPath;

  @Parameter(names = "-DoutPath", required = true)
  private String outPath;

  @Parameter(names = "-DtimeOutPath", required = true)
  private String timeOutPath;

  @Parameter(names = "-Dn")
  private Integer n;

  @Parameter(names = "-Dprov")
  private Integer province;

  private static final ClientArgs singletonInstance = new ClientArgs();

  private ClientArgs() {
  }

  public static ClientArgs getInstance() {
    return singletonInstance;
  }

  public List<String> getAddresses() {
    return addresses;
  }

  public Integer getQuery() {
    return query;
  }

  public String getInPath() {
    return inPath;
  }

  public String getOutPath() {
    return outPath;
  }

  public String getTimeOutPath() {
    return timeOutPath;
  }

  public Integer getN() {
    final int query = getQuery();
    if (2 != query && 6 != query && 7 != query) {
      throw new IllegalStateException("Should getN when query equals 2, 6 or 7. Query = " + query);
    }
    return Objects.requireNonNull(n);
  }

  public Integer getProvince() {
    if (2 != getQuery()) {
      throw new IllegalStateException("Should getProvince when query equals 2");
    }
    return Objects.requireNonNull(province);
  }
}

package ar.edu.itba.pod.census.client.args;

import com.beust.jcommander.IDefaultProvider;
import com.beust.jcommander.Parameter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

@SuppressWarnings({"UnusedDeclaration"}) // JCommander populates all the private fields
public final class ClientArgs {

  private static final ClientArgs SINGLETON_INSTANCE = new ClientArgs();

  public static final IDefaultProvider SYSTEM_PROPERTIES_PROVIDER =
      optionName -> System.getProperties().getProperty(optionName.replaceAll("^-+", ""));

  private static final List<Integer> QUERIES_N = Arrays.asList(2, 6, 7);
  private static final List<Integer> QUERIES_PROVINCE = Collections.singletonList(2);

  @Parameter(names = {"-addresses", "-a"}, required = true)
  private List<String> addresses = new ArrayList<>();

  @Parameter(names = {"-query", "-q"}, validateWith = QueryArgValidator.class, required = true)
  private int query;

  @Parameter(names = {"-inPath", "-i", "-in"}, required = true)
  private String inPath;

  @Parameter(names = {"-outPath", "-o", "-out"}, required = true)
  private String outPath;

  @Parameter(names = {"-timeOutPath", "-times"}, required = true)
  private String timeOutPath;

  @Parameter(names = {"-n"})
  private Integer n;

  @Parameter(names = {"-prov", "-province"})
  private String province;

  @Parameter(names = {"-d", "-debug"})
  private boolean debug;

  private ClientArgs() {
  }

  public static ClientArgs getInstance() {
    return SINGLETON_INSTANCE;
  }

  public List<String> getAddresses() {
    return addresses;
  }

  public int getQuery() {
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

  public int getN() {
    // TODO: move this validation to somewhere else; build() method perhaps?
//    if (!QUERIES_N.contains(query)) {
//      throw new ArgumentsErrorException("N is undefined for this query");
//    }

    return Objects.requireNonNull(n);
  }

  public String getProvince() {
    // TODO: move this validation to somewhere else; build() method perhaps?
//    if (!QUERIES_PROVINCE.contains(query)) {
//      throw new ArgumentsErrorException("Province is undefined for this query");
//    }

    return Objects.requireNonNull(province);
  }

  public boolean getDebug() {
    return debug;
  }
}

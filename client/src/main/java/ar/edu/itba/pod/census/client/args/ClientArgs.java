package ar.edu.itba.pod.census.client.args;

import com.beust.jcommander.IDefaultProvider;
import com.beust.jcommander.Parameter;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings({"UnusedDeclaration"}) // JCommander populates all the private fields
public final class ClientArgs {

  private static final ClientArgs SINGLETON_INSTANCE = new ClientArgs();

  public static final IDefaultProvider SYSTEM_PROPERTIES_PROVIDER =
      optionName -> System.getProperties().getProperty(optionName.replaceAll("^-+", ""));

  @Parameter(names = {"-addresses", "-a"}, required = true,
      splitter = SemicolonParameterSplitter.class)
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

  public Integer getN() {
    return n;
  }

  public String getProvince() {
    return province;
  }

  public boolean getDebug() {
    return debug;
  }
}

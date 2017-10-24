package ar.edu.itba.pod.census;

public final class SharedConfiguration {

  private final static String GROUP_USERNAME = "55309-54126-54182-54045";
  private final static String GROUP_PASSWORD = "55309-54126-54182-54045";
  private final static String MAP_NAME = "55309-54126-54182-54045";

  private SharedConfiguration() {
  }

  public static String getGroupUsername() {
    return GROUP_USERNAME;
  }

  public static String getGroupPassword() {
    return GROUP_PASSWORD;
  }

  public static String getMapName() {
    return MAP_NAME;
  }
}

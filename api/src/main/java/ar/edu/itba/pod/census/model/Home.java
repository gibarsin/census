package ar.edu.itba.pod.census.model;

import java.io.Serializable;
import java.util.Objects;

public class Home implements Serializable {

  private final int homeId;
  private final String departmentName;
  private final String province;
  private final String region;

  public Home(final int homeId, final String departmentName, final String province) {
    this.homeId = homeId;
    this.departmentName = Objects.requireNonNull(departmentName);
    this.province = Objects.requireNonNull(province);
    this.region = Region.fromProvince(province);
  }

  public int getHomeId() {
    return homeId;
  }

  public String getDepartmentName() {
    return departmentName;
  }

  public String getProvince() {
    return province;
  }

  public String getRegion() {
    return region;
  }

  @Override
  public boolean equals(final Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof Home)) {
      return false;
    }

    final Home home = (Home) other;

    return getHomeId() == home.getHomeId();
  }

  @Override
  public int hashCode() {
    return getHomeId();
  }
}

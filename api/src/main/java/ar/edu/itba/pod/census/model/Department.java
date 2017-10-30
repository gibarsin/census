package ar.edu.itba.pod.census.model;

import java.io.Serializable;
import java.util.Objects;

public class Department implements Serializable {

  private final String departmentName;
  private final String province;
  private final String region;

  public Department(final String departmentName, final String province) {
    this.departmentName = Objects.requireNonNull(departmentName);
    this.province = Objects.requireNonNull(province);
    this.region = Region.fromProvince(province);
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
    if (!(other instanceof Department)) {
      return false;
    }

    final Department that = (Department) other;

    if (!getDepartmentName().equals(that.getDepartmentName())) {
      return false;
    }

    return getProvince().equals(that.getProvince());
  }

  @Override
  public int hashCode() {
    return 31 * getDepartmentName().hashCode() + getProvince().hashCode();
  }
}

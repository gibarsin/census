package ar.edu.itba.pod.census.model;

import java.io.Serializable;

public class Container implements Serializable {
  public enum EmploymentStatus {
    UNKNOWN(0), EMPLOYED(1), UNEMPLOYED(2), INACTIVE(3);

    private final int value;

    EmploymentStatus(final int value) {
      this.value = value;
    }

    public static EmploymentStatus valueOf(final int value) {
      for (final EmploymentStatus employmentStatus : EmploymentStatus.values()) {
        if (employmentStatus.value == value) {
          return employmentStatus;
        }
      }

      throw new IllegalArgumentException("Invalid employment status value");
    }
  }

  private final int employmentStatusId;
  private final int homeId;
  private final String departmentName;
  private final String province;

  public Container(final int employmentStatusId, final int homeId,
                   final String departmentName, final String province) {
    this.employmentStatusId = employmentStatusId;
    this.homeId = homeId;
    this.departmentName = departmentName.trim();
    this.province = province.trim();
  }

  public int getEmploymentStatusId() {
    return employmentStatusId;
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

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    final Container container = (Container) o;

    if (employmentStatusId != container.employmentStatusId) return false;
    if (homeId != container.homeId) return false;
    if (departmentName != null ? !departmentName.equals(container.departmentName) : container.departmentName != null)
      return false;
    return province != null ? province.equals(container.province) : container.province == null;
  }

  @Override
  public int hashCode() {
    int result = employmentStatusId;
    result = 31 * result + homeId;
    result = 31 * result + (departmentName != null ? departmentName.hashCode() : 0);
    result = 31 * result + (province != null ? province.hashCode() : 0);
    return result;
  }
}

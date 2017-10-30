package ar.edu.itba.pod.census.model;

import java.io.Serializable;
import java.util.Objects;

public class Citizen implements Serializable {

  private final EMPLOYMENT_STATUS employmentStatus;
  private final int homeId;
  private final String departmentName;
  private final String province;
  private final String region;

  public Citizen(final int employmentStatus, final int homeId, final String departmentName,
      final String province) {
    this.employmentStatus = EMPLOYMENT_STATUS.valueOf(employmentStatus);
    this.homeId = homeId;
    this.departmentName = Objects.requireNonNull(departmentName);
    this.province = Objects.requireNonNull(province);
    this.region = Region.fromProvince(province);
  }

  public EMPLOYMENT_STATUS getEmploymentStatus() {
    return employmentStatus;
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

  public enum EMPLOYMENT_STATUS {
    UNKNOWN(0), EMPLOYED(1), UNEMPLOYED(2), INACTIVE(3);

    private final int value;

    EMPLOYMENT_STATUS(final int value) {
      this.value = value;
    }

    public static EMPLOYMENT_STATUS valueOf(final int value) {
      for (final EMPLOYMENT_STATUS employmentStatus : EMPLOYMENT_STATUS.values()) {
        if (employmentStatus.value == value) {
          return employmentStatus;
        }
      }

      throw new IllegalArgumentException("Invalid employment status value");
    }
  }
}

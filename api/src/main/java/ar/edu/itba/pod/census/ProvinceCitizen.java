package ar.edu.itba.pod.census;

// TODO: This should be a Citizen and include all citizen values
public class ProvinceCitizen {

  private final int employmentStatus;
  private final int homeId;
  private final String departmentName;

  public ProvinceCitizen(final int employmentStatus, final int homeId,
      final String departmentName) {
    this.employmentStatus = employmentStatus;
    this.homeId = homeId;
    this.departmentName = departmentName;
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

      throw new IllegalArgumentException("Invalid value");
    }
  }
}

package ar.edu.itba.pod.census;

public class ProvinceHabitant {

  private final int habitantCondition;
  private final int homeId;
  private final String departmentName;

  public ProvinceHabitant(final int habitantCondition, final int homeId,
      final String departmentName) {
    this.habitantCondition = habitantCondition;
    this.homeId = homeId;
    this.departmentName = departmentName;
  }
}

package ar.edu.itba.pod.census.client;

public class CensusCSVRecords {
  public enum Headers {
    EMPLOYMENT_STATUS(0), HOME_ID(1), DEPARTMENT_NAME(2), PROVINCE_NAME(3);

    private final int column;

    Headers(final int column) {
      this.column = column;
    }

    public int getColumn() {
      return column;
    }
  }
}

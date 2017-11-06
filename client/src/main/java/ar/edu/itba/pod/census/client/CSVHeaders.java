package ar.edu.itba.pod.census.client;

public enum CSVHeaders {
  EMPLOYMENT_STATUS(0), HOME_ID(1), DEPARTMENT_NAME(2), PROVINCE_NAME(3);

  private final int column;

  CSVHeaders(final int column) {
    this.column = column;
  }

  public int getColumn() {
    return column;
  }
}

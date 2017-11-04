package ar.edu.itba.pod.census.model;

import java.io.Serializable;

public class RegionOccupationCombinerWrapper implements Serializable {
  private final int unemployed;
  private final int total;

  public RegionOccupationCombinerWrapper(final int unemployed, final int total) {
    this.unemployed = unemployed;
    this.total = total;
  }

  public int getUnemployed() {
    return unemployed;
  }

  public int getTotal() {
    return total;
  }
}

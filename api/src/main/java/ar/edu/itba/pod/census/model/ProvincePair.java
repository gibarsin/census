package ar.edu.itba.pod.census.model;

import java.io.Serializable;

public class ProvincePair implements Serializable {
  private final String first;
  private final String second;

  public ProvincePair(final String provinceI, final String provinceJ) {
    this.first = provinceI;
    this.second = provinceJ;
  }

  public String getFirst() {
    return first;
  }

  public String getSecond() {
    return second;
  }
}

package ar.edu.itba.pod.census.model;

import java.text.Normalizer;
import java.text.Normalizer.Form;

public enum Region {
  NORTE_GRANDE("Región del Norte Grande Argentino"),
  NUEVO_CUYO("Región del Nuevo Cuyo"),
  CENTRO("Región Centro"),
  BUENOS_AIRES("Región Buenos Aires"),
  PATAGONICA("Región Patagónica");

  private final String stringValue;

  Region(final String stringValue) {
    this.stringValue = stringValue;
  }

  @Override
  public String toString() {
    return Normalizer.normalize(stringValue, Form.NFD);
  }
}

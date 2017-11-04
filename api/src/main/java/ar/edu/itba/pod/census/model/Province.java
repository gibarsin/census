package ar.edu.itba.pod.census.model;

import java.text.Normalizer;
import java.text.Normalizer.Form;

public enum Province {
  BUENOS_AIRES("Buenos Aires", Region.BUENOS_AIRES),
  CABA("Ciudad Autónoma de Buenos Aires", Region.BUENOS_AIRES),
  CATAMARCA("Catamarca", Region.NORTE_GRANDE),
  CHACO("Chaco", Region.NORTE_GRANDE),
  CHUBUT("Chubut", Region.PATAGONICA),
  CORDOBA("Córdoba", Region.CENTRO),
  CORRIENTES("Corrientes", Region.NORTE_GRANDE),
  ENTRE_RIOS("Entre Ríos", Region.CENTRO),
  FORMOSA("Formosa", Region.NORTE_GRANDE),
  JUJUY("Jujuy", Region.NORTE_GRANDE),
  LA_PAMPA("La Pampa", Region.PATAGONICA),
  LA_RIOJA("La Rioja", Region.NUEVO_CUYO),
  MENDOZA("Mendoza", Region.NUEVO_CUYO),
  MISIONES("Misiones", Region.NORTE_GRANDE),
  NEUQUEN("Neuquén", Region.PATAGONICA),
  RIO_NEGRO("Río Negro", Region.PATAGONICA),
  SALTA("Salta", Region.NORTE_GRANDE),
  SAN_JUAN("San Juan", Region.NUEVO_CUYO),
  SAN_LUIS("San Luis", Region.NUEVO_CUYO),
  SANTA_CRUZ("Santa Cruz", Region.PATAGONICA),
  SANTA_FE("Santa Fe", Region.CENTRO),
  SANTIAGO_DEL_ESTERO("Santiago del Estero", Region.NORTE_GRANDE),
  TIERRA_DEL_FUEGO("Tierra del Fuego", Region.PATAGONICA),
  TUCUMAN("Tucumán", Region.NORTE_GRANDE);

  private final String stringValue;
  private final Region region;

  Province(final String stringValue, final Region region) {
    this.stringValue = stringValue;
    this.region = region;
  }

  public Region getRegion() {
    return region;
  }

  @Override
  public String toString() {
    return Normalizer.normalize(stringValue, Form.NFD);
  }

  public static Province fromString(final String desiredProvince) {
    for (final Province province : Province.values()) {
      if (province.stringValue.equalsIgnoreCase(desiredProvince)) {
        return province;
      }
    }

    throw new IllegalArgumentException("No province named " + desiredProvince + " found");
  }
}

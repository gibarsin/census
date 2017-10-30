package ar.edu.itba.pod.census.model;

import java.text.Normalizer;
import java.text.Normalizer.Form;

public enum Province {
  BUENOS_AIRES("buenos aires", Region.BUENOS_AIRES),
  CABA("ciudad autónoma de buenos aires", Region.BUENOS_AIRES),
  CATAMARCA("catamarca", Region.NORTE_GRANDE),
  CHACO("chaco", Region.NORTE_GRANDE),
  CHUBUT("chubut", Region.PATAGONICA),
  CORDOBA("córdoba", Region.CENTRO),
  CORRIENTES("corrientes", Region.NORTE_GRANDE),
  ENTRE_RIOS("entre ríos", Region.CENTRO),
  FORMOSA("formosa", Region.NORTE_GRANDE),
  JUJUY("jujuy", Region.NORTE_GRANDE),
  LA_PAMPA("la pampa", Region.PATAGONICA),
  LA_RIOJA("la rioja", Region.NUEVO_CUYO),
  MENDOZA("mendoza", Region.NUEVO_CUYO),
  MISIONES("misiones", Region.NORTE_GRANDE),
  NEUQUEN("neuquén", Region.PATAGONICA),
  RIO_NEGRO("río negro", Region.PATAGONICA),
  SALTA("salta", Region.NORTE_GRANDE),
  SAN_JUAN("san juan", Region.NUEVO_CUYO),
  SAN_LUIS("san luis", Region.NUEVO_CUYO),
  SANTA_CRUZ("santa cruz", Region.PATAGONICA),
  SANTA_FE("santa fe", Region.CENTRO),
  SANTIAGO_DEL_ESTERO("santiago del estero", Region.NORTE_GRANDE),
  TIERRA_DEL_FUEGO("tierra del fuego", Region.PATAGONICA),
  TUCUMAN("tucumán", Region.NORTE_GRANDE);

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

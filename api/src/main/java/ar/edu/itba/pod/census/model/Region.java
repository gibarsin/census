package ar.edu.itba.pod.census.model;

import java.text.Normalizer;
import java.text.Normalizer.Form;

public enum Region {
  NORTE_GRANDE("región del norte grande argentino"),
  NUEVO_CUYO("región del nuevo cuyo"),
  CENTRO("región centro"),
  BUENOS_AIRES("región buenos aires"),
  PATAGONICA("región patagónica");

  private final String stringValue;

  Region(final String stringValue) {
    this.stringValue = stringValue;
  }

  @Override
  public String toString() {
    return Normalizer.normalize(stringValue, Form.NFD);
  }

  @Deprecated // TODO: Delete this
  public static Region fromProvince(final Province province) {
    switch (province) {
      case JUJUY:
      case SALTA:
      case CATAMARCA:
      case TUCUMAN:
      case SANTIAGO_DEL_ESTERO:
      case CHACO:
      case FORMOSA:
      case CORRIENTES:
      case MISIONES:
        return Region.NORTE_GRANDE;

      case LA_RIOJA:
      case SAN_JUAN:
      case MENDOZA:
      case SAN_LUIS:
        return Region.NUEVO_CUYO;

      case CORDOBA:
      case SANTA_FE:
      case ENTRE_RIOS:
        return Region.CENTRO;

      case BUENOS_AIRES:
      case CABA:
        return Region.BUENOS_AIRES;

      case NEUQUEN:
      case LA_PAMPA:
      case RIO_NEGRO:
      case CHUBUT:
      case SANTA_CRUZ:
      case TIERRA_DEL_FUEGO:
        return Region.PATAGONICA;
    }

    throw new IllegalArgumentException("No region for province " + province + " found");
  }

  @Deprecated // TODO: Delete this
  public static String fromProvince(final String province) {
    switch (province.toLowerCase()) {
      case "jujuy":
      case "salta":
      case "catamarca":
      case "tucumán":
      case "santiago del estero":
      case "chaco":
      case "formosa":
      case "corrientes":
      case "misiones":
        return "Región del Norte Grande Argentino";

      case "la rioja":
      case "san juan":
      case "mendoza":
      case "san luis":
        return "Región del Nuevo Cuyo";

      case "córdoba":
      case "santa fe":
      case "entre ríos":
        return "Región Centro";

      case "buenos aires":
      case "ciudad autónoma de buenos aires":
        return "Región Buenos Aires";

      case "neuquén":
      case "la pampa":
      case "río negro":
      case "chubut":
      case "santa cruz":
      case "tierra del fuego":
        return "Región Patagónica";
    }

    throw new IllegalArgumentException("No region for province " + province + " found");
  }
}

package ar.edu.itba.pod.census.model;

public final class Region {

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

    throw new IllegalArgumentException("Invalid province");
  }
}

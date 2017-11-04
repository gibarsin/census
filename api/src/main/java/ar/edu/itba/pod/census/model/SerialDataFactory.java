package ar.edu.itba.pod.census.model;

import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public class SerialDataFactory implements DataSerializableFactory {

  public static final int FACTORY_ID = 1;
  public static final int CONTAINER_TYPE_ID = 1;
  public static final int REGION_OCCUPATION_COMBINER_WRAPPER_TYPE_ID = 2;

  @Override
  public IdentifiedDataSerializable create(final int typeId) {
    switch (typeId) {
      case CONTAINER_TYPE_ID: {
        return new Container();
      }

      case REGION_OCCUPATION_COMBINER_WRAPPER_TYPE_ID: {
        return new RegionOccupationCombinerWrapper();
      }
    }

    throw new IllegalArgumentException("Invalid type ID");
  }
}

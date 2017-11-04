package ar.edu.itba.pod.census.model;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import java.io.IOException;

public class RegionOccupationCombinerWrapper implements IdentifiedDataSerializable {

  private int unemployed;
  private int total;

  /* package-private */ RegionOccupationCombinerWrapper() {
  }

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

  @Override
  public int getFactoryId() {
    return SerialDataFactory.FACTORY_ID;
  }

  @Override
  public int getId() {
    return SerialDataFactory.REGION_OCCUPATION_COMBINER_WRAPPER_TYPE_ID;
  }

  @Override
  public void writeData(final ObjectDataOutput out) throws IOException {
    out.writeInt(unemployed);
    out.writeInt(total);
  }

  @Override
  public void readData(final ObjectDataInput in) throws IOException {
    this.unemployed = in.readInt();
    this.total = in.readInt();
  }
}

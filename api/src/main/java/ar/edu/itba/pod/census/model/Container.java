package ar.edu.itba.pod.census.model;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import java.io.IOException;

public class Container implements IdentifiedDataSerializable {

  public enum EmploymentStatus {
    UNKNOWN(0), EMPLOYED(1), UNEMPLOYED(2), INACTIVE(3);

    private final int value;

    EmploymentStatus(final int value) {
      this.value = value;
    }

    public static EmploymentStatus valueOf(final int value) {
      for (final EmploymentStatus employmentStatus : EmploymentStatus.values()) {
        if (employmentStatus.value == value) {
          return employmentStatus;
        }
      }

      throw new IllegalArgumentException("Invalid employment status value");
    }
  }

  private int employmentStatusId;
  private int homeId;
  private String departmentName;
  private String province;

  /* package-private */ Container() {
  }

  public Container(final int employmentStatusId, final int homeId,
      final String departmentName, final String province) {
    this.employmentStatusId = employmentStatusId;
    this.homeId = homeId;
    this.departmentName = departmentName.trim();
    this.province = province.trim();
  }

  public int getEmploymentStatusId() {
    return employmentStatusId;
  }

  public int getHomeId() {
    return homeId;
  }

  public String getDepartmentName() {
    return departmentName;
  }

  public String getProvince() {
    return province;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final Container container = (Container) o;

    if (employmentStatusId != container.employmentStatusId) {
      return false;
    }
    if (homeId != container.homeId) {
      return false;
    }
    //noinspection SimplifiableIfStatement
    if (departmentName != null ? !departmentName.equals(container.departmentName)
        : container.departmentName != null) {
      return false;
    }
    return province != null ? province.equals(container.province) : container.province == null;
  }

  @Override
  public int hashCode() {
    int result = employmentStatusId;
    result = 31 * result + homeId;
    result = 31 * result + (departmentName != null ? departmentName.hashCode() : 0);
    result = 31 * result + (province != null ? province.hashCode() : 0);
    return result;
  }

  @Override
  public int getFactoryId() {
    return SerialDataFactory.FACTORY_ID;
  }

  @Override
  public int getId() {
    return SerialDataFactory.CONTAINER_TYPE_ID;
  }

  @Override
  public void writeData(final ObjectDataOutput out) throws IOException {
    out.writeInt(employmentStatusId);
    out.writeInt(homeId);
    out.writeUTF(departmentName);
    out.writeUTF(province);
  }

  @Override
  public void readData(final ObjectDataInput in) throws IOException {
    this.employmentStatusId = in.readInt();
    this.homeId = in.readInt();
    this.departmentName = in.readUTF();
    this.province = in.readUTF();
  }
}

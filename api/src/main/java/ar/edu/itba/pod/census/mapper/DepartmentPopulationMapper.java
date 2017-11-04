package ar.edu.itba.pod.census.mapper;

import ar.edu.itba.pod.census.model.Container;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class DepartmentPopulationMapper implements Mapper<String, Container, String, Integer> {
  private final String province;

  public DepartmentPopulationMapper(final String province) {
    this.province = province;
  }

  @Override
  public void map(final String key, final Container container, final Context<String, Integer> context) {
    if (container.getProvince().equalsIgnoreCase(province)) {
      context.emit(container.getDepartmentName(), 1);
    }
  }
}

package ar.edu.itba.pod.census.mapper;

import ar.edu.itba.pod.census.model.Container;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class PopularDepartmentMapper implements Mapper<Integer, Container, String, String> {
  @Override
  public void map(final Integer key, final Container container, final Context<String, String> context) {
    context.emit(container.getDepartmentName(), container.getProvince());
  }
}

package ar.edu.itba.pod.census.mapper;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class PopularDepartmentNamesMapper implements Mapper<String, String, String, String> {
  @Override
  public void map(final String department, final String province, final Context<String, String> context) {
    context.emit(department, province);
  }
}

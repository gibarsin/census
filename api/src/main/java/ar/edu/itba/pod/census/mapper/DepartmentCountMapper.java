package ar.edu.itba.pod.census.mapper;

import ar.edu.itba.pod.census.model.Department;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class DepartmentCountMapper implements Mapper<String, Department, String, Integer> {

  @Override
  public void map(final String key, final Department department,
      final Context<String, Integer> context) {
    context.emit(department.getDepartmentName(), 1);
  }
}

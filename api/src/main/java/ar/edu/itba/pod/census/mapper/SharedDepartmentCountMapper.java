package ar.edu.itba.pod.census.mapper;

import ar.edu.itba.pod.census.model.Province;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class SharedDepartmentCountMapper implements Mapper<String, Province, String, String> {

  @Override
  public void map(final String departmentName, final Province province,
      final Context<String, String> context) {
    for (final Province otherProvince : Province.values()) {
      final int dif = province.toString().compareToIgnoreCase(otherProvince.toString());

      if (dif < 0) {
        context.emit(province + " + " + otherProvince, departmentName);
      } else if (dif > 0) {
        context.emit(otherProvince + " + " + province, departmentName);
      }
    }
  }
}

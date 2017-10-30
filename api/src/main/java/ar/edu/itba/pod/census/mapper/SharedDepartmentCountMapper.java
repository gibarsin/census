package ar.edu.itba.pod.census.mapper;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import java.util.Set;

public class SharedDepartmentCountMapper implements Mapper<String, Set<String>, String, Integer> {

  @Override
  public void map(final String department, final Set<String> provinces,
      final Context<String, Integer> context) {

    for (final String province1 : provinces) {
      for (final String province2 : provinces) {
        if (!province1.equals(province2)) {
          if (province1.compareToIgnoreCase(province2) > 0) {
            System.out.println("COMPARTEN: " + province1 + " + " + province2);
            context.emit(province1 + " + " + province2, 1);
          }
        }
      }
    }
  }
}

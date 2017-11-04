package ar.edu.itba.pod.census.reducer;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.Set;

public class PopularDepartmentNamesReducerFactory implements ReducerFactory<String, Set<String>, Integer> {
  @Override
  public Reducer<Set<String>, Integer> newReducer(final String s) {
    return new PopularDepartmentNamesReducer();
  }

  private static class PopularDepartmentNamesReducer extends Reducer<Set<String>, Integer> {
    private int countProvinces;

    @Override
    public void beginReduce() {
      countProvinces = 0;
    }

    @Override
    public void reduce(final Set<String> provinces) {
      countProvinces += provinces.size();
    }

    @Override
    public Integer finalizeReduce() {
      return countProvinces;
    }
  }
}

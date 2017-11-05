package ar.edu.itba.pod.census.reducer;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.HashSet;
import java.util.Set;

public class PopularDepartmentNamesReducerFactory implements ReducerFactory<String, Set<String>, Integer> {
  @Override
  public Reducer<Set<String>, Integer> newReducer(final String s) {
    return new PopularDepartmentNamesReducer();
  }

  private static class PopularDepartmentNamesReducer extends Reducer<Set<String>, Integer> {
    private final Set<String> provincesMerge;

    private PopularDepartmentNamesReducer() {
      provincesMerge = new HashSet<>();
    }

    @Override
    public void beginReduce() {
      provincesMerge.clear();
    }

    @Override
    public void reduce(final Set<String> provinces) {
      provincesMerge.addAll(provinces);
    }

    @Override
    public Integer finalizeReduce() {
      return provincesMerge.size();
    }
  }
}

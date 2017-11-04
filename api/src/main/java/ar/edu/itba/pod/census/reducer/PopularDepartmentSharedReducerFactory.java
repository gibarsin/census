package ar.edu.itba.pod.census.reducer;

import ar.edu.itba.pod.census.model.ProvincePair;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.*;

public class PopularDepartmentSharedReducerFactory
        implements ReducerFactory<String, Set<String>, Set<ProvincePair>> {
  @Override
  public Reducer<Set<String>, Set<ProvincePair>> newReducer(final String department) {
    return new PopularDepartmentSharedReducer();
  }

  private static class PopularDepartmentSharedReducer
          extends Reducer<Set<String>, Set<ProvincePair>> {
    private final Set<String> sharedDepartmentsByProvincesPairs;

    private PopularDepartmentSharedReducer() {
      sharedDepartmentsByProvincesPairs = new HashSet<>();
    }

    @Override
    public void beginReduce() {
      sharedDepartmentsByProvincesPairs.clear();
    }

    @Override
    public void reduce(final Set<String> provincesPerDepartment) {
      sharedDepartmentsByProvincesPairs.addAll(provincesPerDepartment);
    }

    @Override
    public Set<ProvincePair> finalizeReduce() {
      final Set<ProvincePair> provincePairs = new HashSet<>();

      final List<String> provinces = new ArrayList<>();
      provinces.addAll(sharedDepartmentsByProvincesPairs);
      final int size = provinces.size();
      for (int i = 0 ; i < size ; i++) {
        for (int j = i + 1 ; j < size ; j++) {
          final String provinceI = provinces.get(i);
          final String provinceJ = provinces.get(j);
          provincePairs.add(new ProvincePair(provinceI, provinceJ));
        }
      }

      return provincePairs;
    }
  }
}

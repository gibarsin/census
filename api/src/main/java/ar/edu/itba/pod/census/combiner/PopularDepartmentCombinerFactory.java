package ar.edu.itba.pod.census.combiner;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

import java.util.HashSet;
import java.util.Set;

public class PopularDepartmentCombinerFactory implements CombinerFactory<String, String, Set<String>> {
  @Override
  public Combiner<String, Set<String>> newCombiner(final String s) {
    return new PopularDepartmentNamesCombiner();
  }

  private class PopularDepartmentNamesCombiner extends Combiner<String, Set<String>> {
    private final Set<String> provinces;

    private PopularDepartmentNamesCombiner() {
      this.provinces = new HashSet<>();
    }

    @Override
    public void reset() {
      provinces.clear();
    }

    @Override
    public void combine(final String province) {
      provinces.add(province);
    }

    @Override
    public Set<String> finalizeChunk() {
      return new HashSet<>(provinces);
    }
  }
}

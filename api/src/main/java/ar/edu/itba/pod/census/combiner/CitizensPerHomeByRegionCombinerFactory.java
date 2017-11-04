package ar.edu.itba.pod.census.combiner;

import ar.edu.itba.pod.census.model.Region;
import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

import java.util.HashMap;
import java.util.Map;

public class CitizensPerHomeByRegionCombinerFactory implements CombinerFactory<Region, Integer, Map<Integer, Integer>> {
  @Override
  public Combiner<Integer, Map<Integer, Integer>> newCombiner(final Region region) {
    return new CitizensPerHomeByRegionCombiner();
  }

  private static class CitizensPerHomeByRegionCombiner extends Combiner<Integer, Map<Integer, Integer>> {
    private final Map<Integer, Integer> counterByHomeId;

    private CitizensPerHomeByRegionCombiner() {
      counterByHomeId = new HashMap<>();
    }

    @Override
    public void reset() {
      counterByHomeId.clear();
    }

    @Override
    public void combine(final Integer homeId) {
      counterByHomeId.compute(homeId, (key, counter) -> counter == null ? 1 : counter + 1);
    }

    @Override
    public Map<Integer, Integer> finalizeChunk() {
      return counterByHomeId;
    }
  }
}

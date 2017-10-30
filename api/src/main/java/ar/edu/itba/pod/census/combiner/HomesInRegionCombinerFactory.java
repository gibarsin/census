package ar.edu.itba.pod.census.combiner;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class HomesInRegionCombinerFactory implements CombinerFactory<String, Integer, Integer> {

  @Override
  public Combiner<Integer, Integer> newCombiner(final String key) {
    return new HomesInRegionCombiner();
  }

  private class HomesInRegionCombiner extends Combiner<Integer, Integer> {

    private int sum = 0;

    @Override
    public void combine(final Integer value) {
      sum += value;
    }

    @Override
    public Integer finalizeChunk() {
      return sum;
    }

    @Override
    public void reset() {
      sum = 0;
    }
  }
}

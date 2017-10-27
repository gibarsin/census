package ar.edu.itba.pod.census.combiner;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class RegionPopulationCombinerFactory implements CombinerFactory<String, Long, Long> {

  @Override
  public Combiner<Long, Long> newCombiner(final String s) {
    return new RegionPopulationCombiner();
  }

  private class RegionPopulationCombiner extends Combiner<Long, Long> {

    private long sum = 0;

    @Override
    public void combine(final Long value) {
      sum += value;
    }

    @Override
    public Long finalizeChunk() {
      return sum;
    }

    @Override
    public void reset() {
      sum = 0;
    }
  }
}

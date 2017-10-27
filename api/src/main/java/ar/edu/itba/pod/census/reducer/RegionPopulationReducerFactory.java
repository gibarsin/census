package ar.edu.itba.pod.census.reducer;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class RegionPopulationReducerFactory implements ReducerFactory<String, Long, Long> {

  @Override
  public Reducer<Long, Long> newReducer(final String s) {
    return new RegionPopulationReducer();
  }

  private class RegionPopulationReducer extends Reducer<Long, Long> {

    private volatile long sum = 0;

    @Override
    public void reduce(final Long value) {
      sum += value;
    }

    @Override
    public Long finalizeReduce() {
      return sum;
    }
  }
}

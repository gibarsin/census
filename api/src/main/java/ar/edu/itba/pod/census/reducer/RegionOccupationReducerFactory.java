package ar.edu.itba.pod.census.reducer;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import java.math.BigDecimal;

public class RegionOccupationReducerFactory implements ReducerFactory<String, Integer, BigDecimal> {

  @Override
  public Reducer<Integer, BigDecimal> newReducer(final String region) {
    return new RegionOccupationReducer();
  }

  private class RegionOccupationReducer extends Reducer<Integer, BigDecimal> {

    private volatile int unemployed = 0;
    private volatile int total = 0;

    @Override
    public void reduce(final Integer value) {
      unemployed += value;
      total++;
    }

    @Override
    public BigDecimal finalizeReduce() {
      return new BigDecimal((double) unemployed / total).setScale(2, BigDecimal.ROUND_HALF_EVEN);
    }
  }
}

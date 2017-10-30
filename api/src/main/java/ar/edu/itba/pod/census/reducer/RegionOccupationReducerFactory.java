package ar.edu.itba.pod.census.reducer;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class RegionOccupationReducerFactory implements ReducerFactory<String, Integer, Double> {

  @Override
  public Reducer<Integer, Double> newReducer(final String region) {
    System.out.println("Reducer for: " + region);
    return new RegionOccupationReducer();
  }

  private class RegionOccupationReducer extends Reducer<Integer, Double> {

    private volatile int unemployed = 0;
    private volatile int total = 0;

    @Override
    public void reduce(final Integer value) {
      unemployed += value;
      total++;
    }

    @Override
    public Double finalizeReduce() {
      return (double) unemployed / total;
    }
  }
}

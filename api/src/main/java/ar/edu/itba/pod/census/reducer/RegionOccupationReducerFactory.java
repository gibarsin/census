package ar.edu.itba.pod.census.reducer;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class RegionOccupationReducerFactory implements ReducerFactory<String, Boolean, Double> {

  @Override
  public Reducer<Boolean, Double> newReducer(final String region) {
    return new RegionOccupationReducer();
  }

  private class RegionOccupationReducer extends Reducer<Boolean, Double> {

    private int unoccupied = 0;
    private int total = 0;

    @Override
    public void reduce(final Boolean isOccupied) {
      total++;
      if (!isOccupied) {
        unoccupied++;
      }
    }

    @Override
    public Double finalizeReduce() {
      return (double) unoccupied / total;
    }
  }
}

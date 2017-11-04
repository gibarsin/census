package ar.edu.itba.pod.census.reducer;

import ar.edu.itba.pod.census.model.Region;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.Map;

public class CitizensPerHomeByRegionReducerFactory implements ReducerFactory<Region, Map<Integer, Integer>, Integer> {

  @Override
  public Reducer<Map<Integer, Integer>, Integer> newReducer(final Region region) {
    return new CitizensPerHomeByRegionReducer();
  }

  private static class CitizensPerHomeByRegionReducer extends Reducer<Map<Integer, Integer>, Integer> {
    private int totalCitizens;
    private int totalHomes;

    @Override
    public void beginReduce() {
      totalCitizens = 0;
      totalHomes = 0;
    }

    @Override
    public void reduce(final Map<Integer, Integer> counterByHomeId) {
      totalCitizens += counterByHomeId.values().size();
      totalHomes += counterByHomeId.size();
    }

    @Override
    public Integer finalizeReduce() {
      return totalCitizens / totalHomes;
    }
  }
}

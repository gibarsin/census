package ar.edu.itba.pod.census.reducer;

import ar.edu.itba.pod.census.model.Region;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import java.util.HashSet;
import java.util.Set;

public class HomesInRegionAverageReducerFactory implements ReducerFactory<Region, Integer, Double> {

  @Override
  public Reducer<Integer, Double> newReducer(final Region region) {
    return new HomesInRegionAverageReducer();
  }

  private class HomesInRegionAverageReducer extends Reducer<Integer, Double> {

    private final Set<Integer> homeIds = new HashSet<>();

    private int citizens;

    @Override
    public void reduce(final Integer homeId) {
      citizens++;
      homeIds.add(homeId);
    }

    @Override
    public Double finalizeReduce() {
      return ((double) citizens) / homeIds.size();
    }
  }
}

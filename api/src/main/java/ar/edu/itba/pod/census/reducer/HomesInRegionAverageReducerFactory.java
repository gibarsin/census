package ar.edu.itba.pod.census.reducer;

import ar.edu.itba.pod.census.model.Region;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;

public class HomesInRegionAverageReducerFactory implements
    ReducerFactory<Region, Integer, BigDecimal> {

  @Override
  public Reducer<Integer, BigDecimal> newReducer(final Region region) {
    return new HomesInRegionAverageReducer();
  }

  private class HomesInRegionAverageReducer extends Reducer<Integer, BigDecimal> {

    private final Set<Integer> homeIds = new HashSet<>();

    private int citizens;

    @Override
    public void reduce(final Integer homeId) {
      citizens++;
      homeIds.add(homeId);
    }

    @Override
    public BigDecimal finalizeReduce() {
      return new BigDecimal((double) citizens / homeIds.size())
          .setScale(2, BigDecimal.ROUND_HALF_EVEN);
    }
  }
}

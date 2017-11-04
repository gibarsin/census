package ar.edu.itba.pod.census.reducer;

import ar.edu.itba.pod.census.model.Region;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.math.BigDecimal;
import java.util.Map;

public class CitizensPerHomeByRegionReducerFactory implements ReducerFactory<Region, Map<Integer, Integer>, BigDecimal> {

  @Override
  public Reducer<Map<Integer, Integer>, BigDecimal> newReducer(final Region region) {
    return new CitizensPerHomeByRegionReducer();
  }

  private static class CitizensPerHomeByRegionReducer extends Reducer<Map<Integer, Integer>, BigDecimal> {
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
    public BigDecimal finalizeReduce() {
      final BigDecimal result;
      if (totalHomes == 0) {
        result = new BigDecimal(0);
      } else {
        result = new BigDecimal((double) totalCitizens / totalHomes);

      }
      return result.setScale(2, BigDecimal.ROUND_HALF_EVEN);
    }
  }
}

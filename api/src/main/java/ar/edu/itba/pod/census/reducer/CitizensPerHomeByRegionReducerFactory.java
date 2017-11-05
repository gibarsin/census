package ar.edu.itba.pod.census.reducer;

import ar.edu.itba.pod.census.model.Region;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

public class CitizensPerHomeByRegionReducerFactory implements ReducerFactory<Region, Map<Integer, Integer>, BigDecimal> {

  @Override
  public Reducer<Map<Integer, Integer>, BigDecimal> newReducer(final Region region) {
    return new CitizensPerHomeByRegionReducer();
  }

  private static class CitizensPerHomeByRegionReducer extends Reducer<Map<Integer, Integer>, BigDecimal> {
    private final Map<Integer, Integer> localCounterByHomeId;
    private int totalCitizens;
    private int totalHomes;

    private CitizensPerHomeByRegionReducer() {
      localCounterByHomeId = new HashMap<>();
    }

    @Override
    public void beginReduce() {
      localCounterByHomeId.clear();
      totalCitizens = 0;
      totalHomes = 0;
    }

    @Override
    public void reduce(final Map<Integer, Integer> counterByHomeId) {
      for (final Map.Entry<Integer, Integer> entry : counterByHomeId.entrySet()) {
        localCounterByHomeId.merge(entry.getKey(), entry.getValue(), (count1, count2) -> count1 + count2);
      }
    }

    @Override
    public BigDecimal finalizeReduce() {
      for (Map.Entry<Integer, Integer> entry : localCounterByHomeId.entrySet()) {
        totalCitizens += entry.getValue();
        totalHomes ++;
      }

      final BigDecimal result;
      if (totalHomes == 0) {
        result = new BigDecimal(0);
      } else {
        result = new BigDecimal(((double) totalCitizens) / totalHomes);
      }
      return result.setScale(2, BigDecimal.ROUND_HALF_EVEN);
    }
  }
}

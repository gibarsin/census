package ar.edu.itba.pod.census.reducer;

import ar.edu.itba.pod.census.model.Region;
import ar.edu.itba.pod.census.model.RegionOccupationCombinerWrapper;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import java.math.BigDecimal;

public class RegionOccupationReducerFactory implements ReducerFactory<Region, RegionOccupationCombinerWrapper, BigDecimal> {

  @Override
  public Reducer<RegionOccupationCombinerWrapper, BigDecimal> newReducer(final Region region) {
    return new RegionOccupationReducer();
  }

  private class RegionOccupationReducer extends Reducer<RegionOccupationCombinerWrapper, BigDecimal> {
    private int unemployed;
    private int total;

    @Override
    public void beginReduce() {
      unemployed = 0;
      total = 0;
    }

    @Override
    public void reduce(final RegionOccupationCombinerWrapper wrapper) {
      unemployed += wrapper.getUnemployed();
      total += wrapper.getTotal();
    }

    @Override
    public BigDecimal finalizeReduce() {
      final BigDecimal result;
      if (total == 0) {
        result = new BigDecimal(0);
      } else {
        result = new BigDecimal(((double) unemployed) / total);
      }
      return result.setScale(2, BigDecimal.ROUND_HALF_EVEN);
    }
  }
}
